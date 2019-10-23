//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "util/c_style_callback.h"

#define ROCKS_VERSION_BUILDER_DEBUG 0

namespace rocksdb {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->fd.largest_seqno != b->fd.largest_seqno) {
    return a->fd.largest_seqno > b->fd.largest_seqno;
  }
  if (a->fd.smallest_seqno != b->fd.smallest_seqno) {
    return a->fd.smallest_seqno > b->fd.smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

#if ROCKS_VERSION_BUILDER_DEBUG
struct VersionBuilderDebugger {
  struct Item {
    size_t deletion, addition, antiquation;
  };
  std::vector<std::pair<int, uint64_t>> deletion;
  std::vector<std::pair<int, FileMetaData>> addition;
  std::vector<std::pair<uint64_t, uint64_t>> antiquation;
  std::vector<Item> pos;
  std::vector<std::pair<int, FileMetaData>> storage;

  VersionBuilderDebugger() {
    pos.emplace_back(Item{0, 0, 0});
  }

  void PushEdit(VersionEdit* edit) {
    auto& edit_deletion = edit->GetDeletedFiles();
    deletion.insert(deletion.end(), edit_deletion.begin(), edit_deletion.end());

    auto& edit_addition = edit->GetNewFiles();
    addition.insert(addition.end(), edit_addition.begin(), edit_addition.end());

    auto& edit_antiquation = edit->GetAntiquation();
    antiquation.insert(antiquation.end(), edit_antiquation.begin(),
                       edit_antiquation.end());

    pos.emplace_back(Item{deletion.size(), addition.size(),
                          antiquation.size()});
  }

  void PushVersion(VersionStorageInfo* vstorage) {
    for (int i = -1; i < vstorage->num_levels(); ++i) {
      for (auto f : vstorage->LevelFiles(i)) {
        storage.emplace_back(i, *f);
        auto& back = storage.back().second;
        back.table_reader_handle = nullptr;
        back.refs = 0;
      }
    }
  }

  void Verify(VersionBuilder::Rep* rep, VersionStorageInfo* vstorage);
};
#else
struct VersionBuilderDebugger {
  void PushEdit(VersionEdit*) {}
  void PushVersion(VersionStorageInfo*) {}
  void Verify(VersionBuilder::Rep*, VersionStorageInfo*) {}
};
#endif


class VersionBuilder::Rep {
  friend VersionBuilderDebugger;
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };
  struct DependenceItem {
    size_t dependence_version;
    size_t skip_gc_version;
    int level;
    FileMetaData* f;
    uint64_t delta_antiquation;
  };
  struct InheritanceItem {
    size_t count;
    DependenceItem* item;
  };

  std::unique_ptr<VersionBuilderDebugger> debugger_;
  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  std::unordered_map<uint64_t, FileMetaData*>* levels_;
  size_t dependence_version_;
  size_t new_deleted_files_;
  std::unordered_map<uint64_t, DependenceItem> dependence_map_;
  std::unordered_map<uint64_t, InheritanceItem> inheritance_counter_;
  // Store states of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::map<int, std::unordered_set<uint64_t>> invalid_levels_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;
  Status status_;

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage, bool enable_debugger = false)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        dependence_version_(0),
        new_deleted_files_(0),
        has_invalid_levels_(false) {
    if (enable_debugger) {
      debugger_.reset(new VersionBuilderDebugger);
    }
    // level -1 used for dependence files
    levels_ = new std::unordered_map<uint64_t, FileMetaData*>[num_levels_];
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();

    for (int level = -1; level < num_levels_; level++) {
      for (auto f : base_vstorage_->LevelFiles(level)) {
        PutSst(f, level);
      }
    }
  }

  ~Rep() {
    for (auto& pair : dependence_map_) {
      if (pair.second.f != nullptr) {
        UnrefFile(pair.second.f);
      }
    }
    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  void PutInheritance(DependenceItem* item) {
    bool replace = inheritance_counter_.count(item->f->fd.GetNumber()) == 0;
    auto emplace = [&](uint64_t file_number) {
      auto ib =
          inheritance_counter_.emplace(file_number, InheritanceItem{1, item});
      if (!ib.second) {
        ++ib.first->second.count;
        if (replace) {
          ib.first->second.item = item;
        }
      }
    };
    for (auto file_number : item->f->prop.inheritance_chain) {
      emplace(file_number);
    }
    emplace(item->f->fd.GetNumber());
  }

  void DelInheritance(FileMetaData* f) {
    auto erase = [&](uint64_t file_number) {
      auto find = inheritance_counter_.find(file_number);
      assert(find != inheritance_counter_.end());
      if (--find->second.count == 0) {
        inheritance_counter_.erase(find);
      }
    };
    for (auto file_number : f->prop.inheritance_chain) {
      erase(file_number);
    }
    erase(f->fd.GetNumber());
  }

  void PutSst(FileMetaData* f, int level) {
    auto ib = dependence_map_.emplace(f->fd.GetNumber(),
                                      DependenceItem{0, 0, level, f, 0});
    f->refs++;
    if (ib.second) {
      PutInheritance(&ib.first->second);
    } else {
      auto& item = ib.first->second;
      item.level = level;
      UnrefFile(item.f);
      item.f = f;
    }
    if (level >= 0) {
      levels_[level].emplace(f->fd.GetNumber(), f);
    }
  }

  void DelSst(uint64_t file_number, int level) {
    assert(dependence_map_.count(file_number) > 0);
    assert(level >= 0);
    dependence_map_[file_number].level = -1;
    levels_[level].erase(file_number);
  }

  DependenceItem* TransFileNumber(uint64_t file_number) {
    auto find = inheritance_counter_.find(file_number);
    if (find != inheritance_counter_.end()) {
      return find->second.item;
    }
    return nullptr;
  }

  void SetDependence(FileMetaData* f, bool recursive, bool finish) {
    for (auto file_number : f->prop.dependence) {
      auto item = TransFileNumber(file_number);
      if (item == nullptr) {
        if (finish) {
          status_ = Status::Aborted("Missing dependence files");
        }
        continue;
      }
      item->dependence_version = dependence_version_;
      if (recursive) {
        item->skip_gc_version = dependence_version_;
        if (item->f != nullptr && !item->f->prop.dependence.empty()) {
          SetDependence(item->f, item->f->prop.purpose == kMapSst, finish);
        }
      }
    }
  }

  void CalculateDependence(bool finish) {
    if (new_deleted_files_ < 65536 && !finish) {
      return;
    }
    ++dependence_version_;
    for (int level = 0; level < num_levels_; ++level) {
      for (auto& pair : levels_[level]) {
        auto file_number = pair.first;
        assert(dependence_map_.count(file_number) > 0);
        assert(inheritance_counter_.count(file_number) > 0);
        auto& item = dependence_map_[file_number];
        assert(item.level >= 0);
        item.dependence_version = dependence_version_;
        item.skip_gc_version = dependence_version_;
        if (!item.f->prop.dependence.empty()) {
          SetDependence(item.f, item.f->prop.purpose == kMapSst, finish);
        }
      }
    }
    for (auto it = dependence_map_.begin(); it != dependence_map_.end(); ) {
      auto& item = it->second;
      if (item.dependence_version == dependence_version_) {
        assert(inheritance_counter_.count(it->first) > 0 &&
               inheritance_counter_.find(it->first)->second.item == &item);
        if (finish) {
          bool is_skip_gc = item.skip_gc_version == dependence_version_;
          if (item.f->is_skip_gc != is_skip_gc) {
            if (item.f->refs > 1 && item.f->is_skip_gc) {
              // if item.f in other versions, that assigning item.f->is_skip_gc
              // to false might let this item participate in GC before current
              // version was installed. It will cause database corruption.
              auto f = new FileMetaData(*item.f);
              f->table_reader_handle = nullptr;
              f->refs = 1;
              UnrefFile(item.f);
              item.f = f;
            }
            item.f->is_skip_gc = is_skip_gc;
          }
        }
        ++it;
      } else {
        DelInheritance(item.f);
        UnrefFile(item.f);
        it = dependence_map_.erase(it);
      }
    }
    if (finish) {
      inheritance_counter_.clear();
    }
    new_deleted_files_ = 0;
  }

  void ApplyAntiquation(FileMetaData* f) {
    auto find = dependence_map_.find(f->fd.GetNumber());
    if (find != dependence_map_.end()) {
      f->num_antiquation += find->second.delta_antiquation;
    }
  }

  void CheckDependence(VersionStorageInfo* vstorage, FileMetaData* f,
                       bool recursive) {
    auto& dependence_map = vstorage->dependence_map();
    for (auto file_number : f->prop.dependence) {
      auto find = dependence_map.find(file_number);
      if (find == dependence_map.end()) {
        fprintf(stderr, "Missing dependence files");
        abort();
      }
      if (recursive && !find->second->prop.dependence.empty()) {
        CheckDependence(vstorage, find->second,
                        find->second->prop.purpose == kMapSst);
      }
    }
  }

  void CheckConsistency(VersionStorageInfo* vstorage) {
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // make sure the files are sorted correctly
    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);
      for (auto f : level_files) {
        if (!f->prop.dependence.empty()) {
          CheckDependence(vstorage, f, f->prop.purpose == kMapSst);
        }
      }
      for (size_t i = 1; i < level_files.size(); i++) {
        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
        if (level == 0) {
          if (!level_zero_cmp_(f1, f2)) {
            fprintf(stderr, "L0 files are not sorted properly");
            abort();
          }

          if (f2->fd.smallest_seqno == f2->fd.largest_seqno) {
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->fd.smallest_seqno;
            if (!(external_file_seqno < f1->fd.largest_seqno ||
                  external_file_seqno == 0)) {
              fprintf(stderr,
                      "L0 file with seqno %" PRIu64 " %" PRIu64
                      " vs. file with global_seqno %" PRIu64 "\n",
                      f1->fd.smallest_seqno, f1->fd.largest_seqno,
                      external_file_seqno);
              abort();
            }
          } else if (f1->fd.smallest_seqno <= f2->fd.smallest_seqno) {
            fprintf(stderr,
                    "L0 files seqno %" PRIu64 " %" PRIu64 " vs. %" PRIu64
                    " %" PRIu64 "\n",
                    f1->fd.smallest_seqno, f1->fd.largest_seqno,
                    f2->fd.smallest_seqno, f2->fd.largest_seqno);
            abort();
          }
        } else {
          if (!level_nonzero_cmp_(f1, f2)) {
            fprintf(stderr, "L%d files are not sorted properly", level);
            abort();
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {
            fprintf(stderr, "L%d have overlapping ranges %s vs. %s\n", level,
                    (f1->largest).DebugString(true).c_str(),
                    (f2->smallest).DebugString(true).c_str());
            abort();
          }
        }
      }
    }
  }

  void CheckConsistencyForDeletes(VersionEdit* /*edit*/, uint64_t number,
                                  int level) {
#ifdef NDEBUG
    if (!base_vstorage_->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // a file to be deleted better exist in the previous version
    bool found = false;
    for (int l = -1; !found && l < num_levels_; l++) {
      const std::vector<FileMetaData*>& base_files =
          base_vstorage_->LevelFiles(l);
      for (size_t i = 0; i < base_files.size(); i++) {
        FileMetaData* f = base_files[i];
        if (f->fd.GetNumber() == number) {
          found = true;
          break;
        }
      }
    }
    // if the file did not exist in the previous version, then it
    // is possibly moved from lower level to higher level in current
    // version
    for (int l = level + 1; !found && l < num_levels_; l++) {
      auto& level_added = levels_[l];
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
        break;
      }
    }

    // maybe this file was added in a previous edit that was Applied
    if (!found) {
      auto& level_added = levels_[level];
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
      }
    }
    if (!found) {
      fprintf(stderr, "not found %" PRIu64 "\n", number);
      abort();
    }
  }

  bool CheckConsistencyForNumLevels() {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }
    for (auto& level : invalid_levels_) {
      if (level.second.size() > 0) {
        return false;
      }
    }
    return true;
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    CheckConsistency(base_vstorage_);
    if (debugger_) {
      debugger_->PushEdit(edit);
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->GetDeletedFiles();
    new_deleted_files_ += del.size();
    for (auto& pair : del) {
      int level = pair.first;
      auto file_number = pair.second;
      if (level < num_levels_) {
        CheckConsistencyForDeletes(edit, file_number, level);
        assert(levels_[level].count(file_number) > 0);
        DelSst(file_number, level);
      } else {
        auto exising = invalid_levels_[level].find(file_number);
        if (exising != invalid_levels_[level].end()) {
          invalid_levels_[level].erase(exising);
        } else {
          // Deleting an non-existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // Add new files
    for (auto& pair : edit->GetNewFiles()) {
      int level = pair.first;
      if (level < num_levels_) {
        FileMetaData* f = new FileMetaData(pair.second);
        assert(f->table_reader_handle == nullptr);
        assert(level < 0 || levels_[level].count(f->fd.GetNumber()) == 0);
        PutSst(f, level);
      } else {
        uint64_t number = pair.second.fd.GetNumber();
        if (invalid_levels_[level].count(number) == 0) {
          invalid_levels_[level].insert(number);
        } else {
          // Creating an already existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    for (auto& pair : edit->GetAntiquation()) {
      auto item = TransFileNumber(pair.first);
      if (item == nullptr) {
        status_ = Status::Aborted("Bad antiquation file number");
      } else {
        item->delta_antiquation += pair.second;
      }
    }

    // shrink files
    CalculateDependence(false);
  }

  // Save the current state in *v.
  void SaveTo(VersionStorageInfo* vstorage) {
    CheckConsistency(base_vstorage_);
    CheckConsistency(vstorage);
    if (debugger_) {
      debugger_->PushVersion(base_vstorage_);
    }

    std::vector<double> read_amp(num_levels_);

    for (int level = 0; level < num_levels_; level++) {
      auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;

      auto& unordered_added_files = levels_[level];
      vstorage->Reserve(level, unordered_added_files.size());

      // Sort files for the level.
      std::vector<FileMetaData*> ordered_added_files;
      ordered_added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        ordered_added_files.push_back(pair.second);
      }
      std::sort(ordered_added_files.begin(), ordered_added_files.end(), cmp);

      for (auto f : ordered_added_files) {
        ApplyAntiquation(f);
        vstorage->AddFile(level, f, info_log_);
        if (level == 0) {
          read_amp[level] += f->prop.read_amp;
        } else {
          read_amp[level] = std::max<int>(read_amp[level], f->prop.read_amp);
        }
      }
    }
    CalculateDependence(true);
    // Handle actual deleted files
    for (auto f : base_vstorage_->LevelFiles(-1)) {
      if (dependence_map_.count(f->fd.GetNumber()) == 0) {
        // f is to-be-deleted table file
        vstorage->RemoveCurrentStats(f);
      }
    }
    for (auto& pair : dependence_map_) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version_ && item.level == -1) {
        ApplyAntiquation(item.f);
        vstorage->AddFile(-1, item.f, info_log_);
      }
    }
    auto exists = [&](FileMetaData* file_metadata) {
      auto find = dependence_map_.find(file_metadata->fd.GetNumber());
      if (find == dependence_map_.end()) {
        return false;
      }
      return find->second.dependence_version == dependence_version_;
    };
    vstorage->ShrinkDependenceMap(&exists, c_style_callback(exists));
    vstorage->set_read_amplification(read_amp);

    CheckConsistency(vstorage);
    if (debugger_) {
      debugger_->Verify(this, vstorage);
    }
  }

  void LoadTableHandlers(InternalStats* internal_stats,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor,
                         int max_threads) {
    assert(table_cache_ != nullptr);
    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level]) {
        auto* file_meta = file_meta_pair.second;
        if (file_meta->table_reader_handle == nullptr) {
          files_meta.emplace_back(file_meta, level);
        }
      }
    }
    for (auto& pair : dependence_map_) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version_ && item.level == -1 &&
          item.f->table_reader_handle == nullptr) {
        files_meta.emplace_back(item.f, -1);
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        auto file_read_hist =
            level > 0 ? internal_stats->GetFileReadHist(level) : nullptr;
        table_cache_->FindTable(
            env_options_, *(base_vstorage_->InternalComparator()),
            file_meta->fd, &file_meta->table_reader_handle, prefix_extractor,
            false /*no_io */, true /* record_read_stats */,
            file_read_hist, false, level, prefetch_index_and_filter_in_cache);
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage,
                   true/* enable_debugger */)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

void VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  rep_->CheckConsistency(vstorage);
}

void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }

void VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  rep_->SaveTo(vstorage);
}

void VersionBuilder::LoadTableHandlers(InternalStats* internal_stats,
                                       bool prefetch_index_and_filter_in_cache,
                                       const SliceTransform* prefix_extractor,
                                       int max_threads) {
  rep_->LoadTableHandlers(internal_stats,
                          prefetch_index_and_filter_in_cache, prefix_extractor,
                          max_threads);
}


#if ROCKS_VERSION_BUILDER_DEBUG
void VersionBuilderDebugger::Verify(VersionBuilder::Rep* rep,
                                    VersionStorageInfo* vstorage) {
  auto get_edit = [this](size_t i, VersionEdit* edit) {
    if (i == 0) {
      for (auto& pair : storage) {
        edit->AddFile(pair.first, pair.second);
      }
    } else {
      auto begin = pos[i - 1], end = pos[i];
      for (size_t j = begin.deletion; j < end.deletion; ++j) {
        auto& pair = deletion[j];
        edit->DeleteFile(pair.first, pair.second);
      }
      for (size_t j = begin.addition; j < end.addition; ++j) {
        auto& pair = addition[j];
        edit->AddFile(pair.first, pair.second);
      }
      std::unordered_map<uint64_t, uint64_t> antiquation_map;
      for (size_t j = begin.antiquation; j < end.antiquation; ++j) {
        antiquation_map.emplace(antiquation[j]);
      }
      edit->SetAntiquation(antiquation_map);
    }
  };
  auto verify = [rep](VersionStorageInfo* l,
                   VersionStorageInfo* r) -> std::string {
    auto eq = [](FileMetaData* fl, FileMetaData* fr) {
      return fl->fd.GetNumber() == fr->fd.GetNumber() &&
             fl->num_antiquation == fr->num_antiquation;
    };
    auto lt = [](FileMetaData* fl, FileMetaData* fr) {
      return fl->fd.GetNumber() != fr->fd.GetNumber() ?
             fl->fd.GetNumber() < fr->fd.GetNumber() :
             fl->num_antiquation < fr->num_antiquation;
    };
    using cmp = std::function<bool(FileMetaData*, FileMetaData*)>;
    auto debug_show = [&](const std::vector<FileMetaData*>& l_sst,
                          const std::vector<FileMetaData*>& r_sst,
                          const cmp& c) {
      std::vector<FileMetaData*> l_diff, r_diff;
      std::set_difference(l_sst.begin(), l_sst.end(), r_sst.begin(),
                          r_sst.end(), std::back_inserter(l_diff), c);
      std::set_difference(r_sst.begin(), r_sst.end(), l_sst.begin(),
                          l_sst.end(), std::back_inserter(r_diff), c);
      fprintf(stderr, "Diff %zd, %zd", l_diff.size(), r_diff.size());
    };
    for (int i = 0; i < l->num_levels(); ++i) {
      if (l->LevelFiles(i).size() != r->LevelFiles(i).size() ||
          std::mismatch(l->LevelFiles(i).begin(), l->LevelFiles(i).end(),
                        r->LevelFiles(i).begin(), eq).first !=
          l->LevelFiles(i).end()) {
        debug_show(l->LevelFiles(i), r->LevelFiles(i),
                   i == 0 ? rep->level_zero_cmp_ : rep->level_nonzero_cmp_);
        char buffer[32];
        snprintf(buffer, sizeof buffer, "Level %d", i);
        return buffer;
      }
    }
    auto l_sst = l->LevelFiles(-1), r_sst = r->LevelFiles(-1);
    std::sort(l_sst.begin(), l_sst.end(), lt);
    std::sort(r_sst.begin(), r_sst.end(), lt);
    if (l_sst.size() != r_sst.size() ||
        std::mismatch(l_sst.begin(), l_sst.end(), r_sst.begin(), eq).first !=
        l_sst.end()) {
      debug_show(l_sst, r_sst, lt);
      return "Level -1";
    }
    std::vector<std::pair<uint64_t, uint64_t>> l_dep, r_dep;
    for (auto& pair : l->dependence_map()) {
      l_dep.emplace_back(pair.first, pair.second->fd.GetNumber());
    }
    for (auto& pair : r->dependence_map()) {
      r_dep.emplace_back(pair.first, pair.second->fd.GetNumber());
    }
    std::sort(l_dep.begin(), l_dep.end());
    std::sort(r_dep.begin(), r_dep.end());
    if (l_dep != r_dep) {
      return "Dependence";
    }
    return std::string();
  };

  bool has_err = false;
  for (size_t i = 1; i < pos.size(); ++i) {
    VersionStorageInfo vstorage_0(vstorage->InternalComparator(),
                              vstorage->InternalComparator()->user_comparator(),
                              vstorage->num_levels(),
                              kCompactionStyleNone, nullptr, true);
    VersionStorageInfo vstorage_1(vstorage->InternalComparator(),
                              vstorage->InternalComparator()->user_comparator(),
                              vstorage->num_levels(),
                              kCompactionStyleNone, nullptr, true);
    VersionBuilder::Rep rep_0(rep->env_options_, rep->info_log_,
                              rep->table_cache_, &vstorage_1);
    VersionBuilder::Rep rep_1(rep->env_options_, rep->info_log_,
                              rep->table_cache_, &vstorage_0);
    for (size_t j = 0; j < i; ++j) {
      VersionEdit edit;
      get_edit(j, &edit);
      rep_0.Apply(&edit);
    }
    rep_0.SaveTo(&vstorage_0);
    for (size_t j = i; j < pos.size(); ++j) {
      VersionEdit edit;
      get_edit(j, &edit);
      rep_1.Apply(&edit);
    }
    rep_1.SaveTo(&vstorage_1);
    auto err = verify(vstorage, &vstorage_1);
    if (!err.empty()) {
      has_err = true;
      fprintf(stderr, "VersionBuilder debug verify fail : edit count = %zd, "
                      "break = %zd, error = %s\n", pos.size() - 1, i,
                      err.c_str());
    }
    for (int j = -1; j < vstorage->num_levels(); ++j) {
      for (auto f : vstorage_0.LevelFiles(j)) {
        --f->refs;
      }
      for (auto f : vstorage_1.LevelFiles(j)) {
        --f->refs;
      }
    }
  }
  if (has_err) {
    abort();
  }
}
#endif

}  // namespace rocksdb
