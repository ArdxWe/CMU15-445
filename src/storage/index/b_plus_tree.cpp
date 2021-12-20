//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>
#include <string>
#include <utility>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt = 0;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size - 1),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // find leaf page
  LeafPage *target = reinterpret_cast<LeafPage *>(FindLeafPage(key, false, OpType::READ, transaction));

  ValueType v;
  // find value
  auto ret = target->Lookup(key, &v, comparator_);
  result->push_back(v);

  FreePagesInTransaction(false, transaction, target->GetPageId());

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LockRootPageId(true);
  // std::cout << "Insert" << std::endl;
  if (IsEmpty()) {
    StartNewTree(key, value);
    TryUnlockRootPageId(true);
    return true;
  }
  // std::cout << "After start new tree" << std::endl;

  TryUnlockRootPageId(true);
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // ask for new page
  page_id_t newPageId;

  // page id is just disk four KB index
  Page *rootPage = buffer_pool_manager_->NewPage(&newPageId);
  if (rootPage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "StartNewTree out of memory");
  }
  // std::cout <<"fuck" << std::endl;

  // ignore page META data
  LeafPage *root = reinterpret_cast<LeafPage *>(rootPage->GetData());

  // update b+ tree's root page id
  root->Init(newPageId, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = newPageId;

  // not zero means that we create a new record in header page
  UpdateRootPageId(1);

  // insert into leaf page, we don't care split in this, because we are starting a new tree
  root->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(newPageId, true /* write */);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // get leaf page
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(FindLeafPage(key, false, OpType::INSERT, transaction));

  ValueType v;
  bool exist = leafPage->Lookup(key, &v, comparator_);
  if (exist) {
    FreePagesInTransaction(true, transaction);
    return false;
  }
  // std::cout << "insert into leaf" << std::endl;

  int cur_size = leafPage->Insert(key, value, comparator_);
  // std::cout << "after real insert" << std::endl;
  // after we insert into leaf page, if size > maxsize, we should split
  if (cur_size == leafPage->GetMaxSize() + 1) {
    LeafPage *newLeafPage = Split<LeafPage>(leafPage, transaction);

    // set max size
    newLeafPage->SetMaxSize(leaf_max_size_ - 1);
    InsertIntoParent(leafPage, newLeafPage->KeyAt(0), newLeafPage, transaction);
  }

  FreePagesInTransaction(true, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  // ask for new page
  page_id_t newPageId;
  // std::cout << "split before" << std::endl;
  Page *newPage = buffer_pool_manager_->NewPage(&newPageId);

  // std::cout << "split after" << std::endl;

  newPage->WLatch();
  transaction->AddIntoPageSet(newPage);

  if (newPage == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Split out of memory.");
  }

  // move half of pairs to new page
  N *newNode = reinterpret_cast<N *>(newPage->GetData());

  // inherit parent page id
  newNode->Init(newPageId, node->GetParentPageId());
  node->MoveHalfTo(newNode, buffer_pool_manager_);
  // we should unpin outside
  return newNode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *newPage = buffer_pool_manager_->NewPage(&root_page_id_);
    if (newPage == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertIntoParent out of memory");
    }

    InternalPage *newRoot = reinterpret_cast<InternalPage *>(newPage->GetData());
    newRoot->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    newRoot->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // link to parent
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // update
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(newRoot->GetPageId(), true);
    return;
  }

  page_id_t parentId = old_node->GetParentPageId();
  auto *page = FetchPage(parentId);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page);

  new_node->SetParentPageId(parentId);

  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // for internal page, maxsize doesn't include key[0]
  if (parent->GetSize() == parent->GetMaxSize() + 1) {
    InternalPage *newInternalPage = Split(parent, transaction);

    newInternalPage->SetMaxSize(internal_max_size_);
    InsertIntoParent(parent, newInternalPage->KeyAt(0), newInternalPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(parentId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // b plus tree is empty
  if (IsEmpty()) {
    return;
  }

  // get target leaf page
  LeafPage *deleteTarget = reinterpret_cast<LeafPage *>(FindLeafPage(key, false, OpType::DELETE, transaction));

  int nowSize = deleteTarget->RemoveAndDeleteRecord(key, comparator_);

  if (nowSize < deleteTarget->GetMinSize()) {
    CoalesceOrRedistribute<LeafPage>(deleteTarget, transaction);
  }

  FreePagesInTransaction(true, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // node is root
  // case 1: node is leaf, min size = 1, so now size should be 0
  // case 2: node is internal, min size = 2, so now size should be 1
  if (node->IsRootPage()) {
    bool delOldRoot = AdjustRoot(node);  // make the child of N the new root of the tree and delete N
    if (delOldRoot) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return delOldRoot;
  }

  N *otherNode = nullptr;

  bool nPrev = FindSibling(node, &otherNode, transaction);

  auto *parent = FetchPage(node->GetParentPageId());

  InternalPage *parentPage = reinterpret_cast<InternalPage *>(parent);

  // for leaf page and internal page, max size = max size
  if (node->GetSize() + otherNode->GetSize() <= node->GetMaxSize()) {
    // other node, node
    if (nPrev) {
      N *temp = node;
      node = otherNode;
      otherNode = temp;
    }

    // parent remove index
    int removeIndex = parentPage->ValueIndex(node->GetPageId());

    Coalesce(&otherNode, &node, &parentPage, removeIndex, transaction);

    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    return true;
  }

  int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
  Redistribute(otherNode, node, nodeInParentIndex);

  buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // ensure we could coalesce
  assert((*neighbor_node)->GetSize() + (*node)->GetSize() <= (*node)->GetMaxSize());

  (*node)->MoveAllTo(*neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);

  transaction->AddIntoDeletedPageSet((*node)->GetPageId());

  (*parent)->Remove(index);
  if ((*parent)->GetSize() <= (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute<InternalPage>(*parent, transaction);
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    // order: node neighbor_node
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    // order: neighbor_node node
    neighbor_node->MoveLastToFrontOf(node, buffer_pool_manager_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N **sibling_pointer /* output */, Transaction *transaction) {
  auto *page = FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(page);

  // index value is point to node
  int index = parent->ValueIndex(node->GetPageId());

  // prev
  int siblingIndex = index - 1;

  // no prev
  if (index == 0) {
    // next (internal should bigger than 0)
    siblingIndex = 1;
  }

  // output
  *sibling_pointer =
      reinterpret_cast<N *>(CrabingProtocalFetchPage(parent->ValueAt(siblingIndex), OpType::DELETE, -1, transaction));

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);

  // node, sibling ?
  return index == 0;
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    // case 2

    // root and leaf page min size == 1
    // so now size should be 0
    assert(old_root_node->GetSize() == 0);

    // this node should be parent !
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);

    // update root page
    root_page_id_ = INVALID_PAGE_ID;

    // update header page record instead of insert.
    UpdateRootPageId();

    // empty, should delete root page
    return true;
  }

  // root and internal page min size == 2
  // so now size should be 1
  assert(old_root_node->GetSize() == 1);

  InternalPage *root = reinterpret_cast<InternalPage *>(old_root_node);

  // only child id should be new root page id
  page_id_t newRoot = root->RemoveAndReturnOnlyChild();
  root_page_id_ = newRoot;
  UpdateRootPageId();

  auto *page = buffer_pool_manager_->FetchPage(newRoot);

  // ensure logic is right
  assert(page != nullptr);

  // set parent page id of new root page is invalid
  root = reinterpret_cast<InternalPage *>(page->GetData());
  root->SetParentPageId(INVALID_PAGE_ID);

  buffer_pool_manager_->UnpinPage(newRoot, true);

  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key);
  TryUnlockRootPageId(false);

  LeafPage *start_leaf = reinterpret_cast<LeafPage *>(page);
  if (start_leaf == nullptr) {
    return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
  }
  int idx = start_leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, OpType op, Transaction *transaction) {
  // std::cout << "Find leaf page " << std::endl;
  bool exclusive = (op != OpType::READ);
  LockRootPageId(exclusive);
  // std::cout << "locked " << std::endl;
  if (IsEmpty()) {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }

  // std::cout << "fuck" << std::endl;
  // fetch root page id
  auto pointer = CrabingProtocalFetchPage(root_page_id_, op, -1, transaction);

  // get leaf page
  page_id_t next = INVALID_PAGE_ID;
  for (page_id_t cur = root_page_id_; !pointer->IsLeafPage();
       pointer = CrabingProtocalFetchPage(next, op, cur, transaction), cur = next) {
    InternalPage *internalPage = reinterpret_cast<InternalPage *>(pointer);
    if (leftMost) {
      next = internalPage->ValueAt(0);
    } else {
      next = internalPage->Lookup(key, comparator_);
    }
  }

  // maybe we should result Page* type
  return reinterpret_cast<Page *>(pointer);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(page_id_t page_id, OpType op, page_id_t previous,
                                                        Transaction *transaction) {
  // std::cout << "crab fetch page id " << page_id << std::endl;
  bool write = op != OpType::READ;

  auto *page = buffer_pool_manager_->FetchPage(page_id);
  // std::cout << "CrabingProtocalFetchPage" << std::endl;
  Lock(write, page);

  // std::cout << "CrabingProtocalFetchPage after" << std::endl;
  BPlusTreePage *res = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (previous > 0 && (!write || res->IsSafe(op))) {
    FreePagesInTransaction(write, transaction, previous);
  }

  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool write, Transaction *transaction, page_id_t cur) {
  TryUnlockRootPageId(write);
  if (transaction == nullptr) {
    assert(!write && cur >= 0);

    Unlock(false, cur);
    buffer_pool_manager_->UnpinPage(cur, false);
    return;
  }

  for (Page *page : *(transaction->GetPageSet())) {
    page_id_t curPageId = page->GetPageId();
    Unlock(write, page);

    buffer_pool_manager_->UnpinPage(curPageId, write);

    if (transaction->GetDeletedPageSet()->find(curPageId) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(curPageId);
      transaction->GetDeletedPageSet()->erase(curPageId);
    }
  }

  assert(transaction->GetDeletedPageSet()->empty());

  transaction->GetPageSet()->clear();
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
