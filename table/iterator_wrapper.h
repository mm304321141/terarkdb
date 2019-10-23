//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <set>

#include "table/internal_iterator.h"

namespace rocksdb {

class SeparateHelper;

// A internal wrapper class with an interface similar to Iterator that caches
// the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
template <class TValue = LazySlice>
class IteratorWrapperBase {
 public:
  IteratorWrapperBase() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapperBase(InternalIteratorBase<TValue>* _iter)
      : iter_(nullptr) {
    Set(_iter);
  }
  ~IteratorWrapperBase() {}
  InternalIteratorBase<TValue>* iter() const { return iter_; }

  // Set the underlying Iterator to _iter and return
  // previous underlying Iterator.
  InternalIteratorBase<TValue>* Set(InternalIteratorBase<TValue>* _iter) {
    InternalIteratorBase<TValue>* old_iter = iter_;

    iter_ = _iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
    return old_iter;
  }

  void SetValid(bool valid) { valid_ = valid; }

  void DeleteIter(bool is_arena_mode) {
    if (iter_) {
      if (!is_arena_mode) {
        delete iter_;
      } else {
        iter_->~InternalIteratorBase<TValue>();
      }
    }
  }

  // Iterator interface methods
  bool Valid() const        { return valid_; }
  Slice key() const         { assert(Valid()); return key_; }
  TValue value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekForPrev(const Slice& k) {
    assert(iter_);
    iter_->SeekForPrev(k);
    Update();
  }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 protected:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
      assert(iter_->status().ok());
    }
  }

  InternalIteratorBase<TValue>* iter_;
  bool valid_;
  Slice key_;
};

using IteratorWrapper = IteratorWrapperBase<LazySlice>;

class CombinedInternalIterator : public InternalIterator {
 public:
  CombinedInternalIterator(InternalIterator* iter,
                           const SeparateHelper* separate_helper)
      : iter_(iter), separate_helper_(separate_helper) {}

  bool Valid() const override { return iter_->Valid(); }
  Slice key() const override { return iter_->key(); }
  LazySlice value() const override;
  Status status() const override { return iter_->status(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  void Seek(const Slice& k) override { iter_->Seek(k); }
  void SeekForPrev(const Slice& k) override { iter_->SeekForPrev(k); }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }

  const SeparateHelper* separate_helper() const { return separate_helper_; }

  InternalIterator* iter_;
  const SeparateHelper* separate_helper_;
};

class LazyInternalIteratorWrapper : public InternalIterator {
 public:
  LazyInternalIteratorWrapper(
      InternalIterator*(*new_iter_callback)(void*), void* arg,
      const std::atomic<bool>* shutting_down = nullptr)
  : new_iter_callback_(new_iter_callback),
    arg_(arg),
    shutting_down_(shutting_down) {}

  bool Valid() const override { return iter_ && iter_->Valid(); }
  Slice key() const override { assert(iter_); return iter_->key(); }
  LazySlice value() const override { assert(iter_); return iter_->value(); }
  Status status() const override {
    if (!iter_) {
      return Status::OK();
    }
    if (shutting_down_ != nullptr && *shutting_down_) {
      return Status::ShutdownInProgress();
    }
    return iter_->status();
  }
  void Next() override { assert(iter_); iter_->Next(); }
  void Prev() override { assert(iter_); iter_->Prev(); }
  void Seek(const Slice& k) override { Init(); iter_->Seek(k); }
  void SeekForPrev(const Slice& k) override { Init(); iter_->SeekForPrev(k); }
  void SeekToFirst() override { Init(); iter_->SeekToFirst(); }
  void SeekToLast() override { Init(); iter_->SeekToLast(); }

  void Reset() {
    iter_.reset();
  }

 private:
  void Init() {
    if (!iter_) {
      iter_.reset(new_iter_callback_(arg_));
    }
  }
  InternalIterator*(*new_iter_callback_)(void*);
  void* arg_;
  const std::atomic<bool>* shutting_down_;
  std::unique_ptr<InternalIterator> iter_;
};

class SeparateValueCollector {
  const SeparateHelper* separate_helper_;
  std::unordered_map<uint64_t, uint64_t>* delta_antiquation_;

public:
  SeparateValueCollector()
      : separate_helper_(nullptr),
        delta_antiquation_(nullptr) {}

  SeparateValueCollector(
      const SeparateHelper* _separate_helper,
      std::unordered_map<uint64_t, uint64_t>* _delta_antiquation)
      : separate_helper_(_separate_helper),
        delta_antiquation_(_delta_antiquation) {}

  LazySlice value(InternalIterator* iter, const Slice& user_key) const;

  LazySlice add(InternalIterator* iter, const Slice& user_key);
  void sub(uint64_t file_number);

  const SeparateHelper* separate_helper() const { return separate_helper_; }
};

}  // namespace rocksdb
