//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <cassert>

#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *BufferPoolManager);
  ~IndexIterator();
  bool isEnd() { return (leaf_ == nullptr) || (index_ >= leaf_->GetSize()); }

  const MappingType &operator*() {
    assert(leaf_ != nullptr);

    return leaf_->GetItem(index_);
  }

  IndexIterator &operator++() {
    index_++;
    if (index_ >= leaf_->GetSize()) {
      page_id_t next = leaf_->GetNextPageId();
      UnlockAndUnPin();

      if (next == INVALID_PAGE_ID) {
        leaf_ = nullptr;
      } else {
        Page *page = bufferPoolManager_->FetchPage(next);
        page->RLatch();
        leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
        index_ = 0;
      }
    }
    return *this;
  }

  bool operator==(const IndexIterator &itr) const { return this->leaf_ == itr.leaf_; }

  bool operator!=(const IndexIterator &itr) const { return this->leaf_ != itr.leaf_; }

 private:
  void UnlockAndUnPin() {
    bufferPoolManager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    bufferPoolManager_->UnpinPage(leaf_->GetPageId(), false);
  }

  // add your own private member variables here
  int index_ = 0;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_ = nullptr;
  BufferPoolManager *bufferPoolManager_ = nullptr;
};

}  // namespace bustub
