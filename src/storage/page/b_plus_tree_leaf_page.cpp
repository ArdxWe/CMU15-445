//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() >= 0);

  // leaf page start from zero
  int st = 0;
  int ed = GetSize() - 1;

  // find the last key in array < input
  while (st <= ed) {
    int mid = (ed - st) / 2 + st;
    if (comparator(array[mid].first, key) >= 0) {
      ed = mid - 1;
    } else {
      st = mid + 1;
    }
  }

  // the first >= key
  return ed + 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) { return array[index]; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int idx = KeyIndex(key, comparator);
  // std::cout << "insert " << std::endl;

  assert(idx >= 0);

  // insert, we don't care split in this
  IncreaseSize(1);
  int curSize = GetSize();
  for (int i = curSize - 1; i > idx; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }
  array[idx].first = key;
  array[idx].second = value;

  return curSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *bufferPoolManager) {
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);
  // copy last half
  int copyIdx = total / 2;  // 7 is 4, 5, 6, 7; 8 is 5, 6, 7, 8
  for (int i = copyIdx; i < total; i++) {
    recipient->array[i - copyIdx].first = array[i].first;
    recipient->array[i - copyIdx].second = array[i].second;
  }

  // link
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());

  // copyIdx <= total - copyIdx
  SetSize(copyIdx);
  recipient->SetSize(total - copyIdx);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int idx = KeyIndex(key, comparator);

  if (idx < GetSize() && comparator(array[idx].first, key) == 0) {
    *value = array[idx].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  // >=
  int index = KeyIndex(key, comparator);

  // >
  if (index >= GetSize() || comparator(key, KeyAt(index)) != 0) {
    return GetSize();
  }

  // =, remove
  for (size_t i = index; i < static_cast<size_t>(GetSize() - 1); i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }

  IncreaseSize(-1);

  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(GetSize() + recipient->GetSize() <= recipient->GetMaxSize());

  int startIdx = recipient->GetSize();  // 7 is 4, 5, 6, 7; 8 is 4, 5, 6, 7, 8
  for (int i = 0; i < GetSize(); i++) {
    recipient->array[startIdx + i].first = array[i].first;
    recipient->array[startIdx + i].second = array[i].second;
  }

  (recipient->array[startIdx]).first = middle_key;

  // set next page
  recipient->SetNextPageId(GetNextPageId());
  // set size, is odd, bigger is last part
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                                  BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);

  for (size_t i = 0; i < static_cast<size_t>(GetSize() - 1); i++) {
    array[i].first = array[i + 1].first;
    array[i].second = array[i + 1].second;
  }

  recipient->CopyLastFrom(pair);

  // update parent
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

  // parent key should >= value page min key
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                                   BufferPoolManager *buffer_pool_manager) {}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
