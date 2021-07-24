//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {
namespace {
using std::lock_guard;
using std::mutex;
using std::string;
};  // namespace

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  lock_guard<mutex> lock{latch_};

  auto iterator = page_table_.find(page_id);
  if (iterator != page_table_.end()) {
    if ((pages_ + iterator->second)->page_id_ == page_id) {
      ((pages_ + iterator->second)->pin_count_)++;
      replacer_->Pin(iterator->second);
      return pages_ + iterator->second;
    }
    // wrong items.
    page_table_.erase(iterator->first);
  }

  frame_id_t f_id;
  if (!free_list_.empty()) {
    f_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&f_id)) {
      return nullptr;
    } else {
      assert((pages_ + f_id)->page_id_ != INVALID_PAGE_ID);
    }
  }

  // lazy write.
  if ((pages_ + f_id)->page_id_ != INVALID_PAGE_ID) {
    page_table_.erase((pages_ + f_id)->page_id_);
    if ((pages_ + f_id)->IsDirty()) {
      disk_manager_->WritePage((pages_ + f_id)->page_id_, (pages_ + f_id)->data_);
    }
  }

  disk_manager_->ReadPage(page_id, (pages_ + f_id)->data_);

  (pages_ + f_id)->page_id_ = page_id;
  (pages_ + f_id)->is_dirty_ = false;
  ((pages_ + f_id)->pin_count_)++;

  page_table_[page_id] = f_id;
  replacer_->Pin(f_id);
  return pages_ + f_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> lock{latch_};
  auto iterator = page_table_.find(page_id);
  if (iterator == page_table_.end()) {
    return true;
  }
  if ((pages_ + iterator->second)->page_id_ != page_id) {
    page_table_.erase(iterator->first);
    return true;
  }
  // dirty must be or old value
  (pages_ + iterator->second)->is_dirty_ = is_dirty || (pages_ + iterator->second)->is_dirty_;

  bool flag = false;
  if ((pages_ + iterator->second)->pin_count_ > 0) {
    ((pages_ + iterator->second)->pin_count_)--;
    // pin count == 0, then remove from page table.
    if ((pages_ + iterator->second)->pin_count_ == 0) {
      if ((pages_ + iterator->second)->is_dirty_) {
        disk_manager_->WritePage((pages_ + iterator->second)->page_id_, (pages_ + iterator->second)->data_);
      }
      replacer_->Unpin(iterator->second);
    }
    flag = true;
  }
  return flag;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  lock_guard<mutex> lock{latch_};
  auto iterator = page_table_.find(page_id);
  if (iterator == page_table_.end()) {
    return false;
  }
  if ((pages_ + iterator->second)->page_id_ != page_id) {
    return false;
  }

  disk_manager_->WritePage((pages_ + iterator->second)->page_id_, (pages_ + iterator->second)->data_);
  (pages_ + iterator->second)->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  lock_guard<mutex> lock{latch_};

  frame_id_t f_id;
  if (!free_list_.empty()) {
    f_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&f_id)) {
      return nullptr;
    }
  }
  if ((pages_ + f_id)->page_id_ != INVALID_PAGE_ID) {
    if ((pages_ + f_id)->IsDirty()) {
      disk_manager_->WritePage((pages_ + f_id)->page_id_, (pages_ + f_id)->data_);
    }
    page_table_.erase((pages_ + f_id)->page_id_);
  }

  *page_id = disk_manager_->AllocatePage();
  (pages_ + f_id)->page_id_ = *page_id;
  (pages_ + f_id)->pin_count_ = 1;
  (pages_ + f_id)->is_dirty_ = false;

  replacer_->Pin(f_id);
  page_table_[*page_id] = f_id;
  return pages_ + f_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  lock_guard<mutex> lock{latch_};

  disk_manager_->DeallocatePage(page_id);
  auto iterator = page_table_.find(page_id);
  bool flag = false;
  if (iterator == page_table_.end()) {
    flag = true;
  } else if ((pages_ + iterator->second)->page_id_ != page_id) {
    flag = true;
  } else {
    if ((pages_ + iterator->second)->pin_count_ != 0) {
      flag = false;
    } else {
      (pages_ + iterator->second)->page_id_ = INVALID_PAGE_ID;
      (pages_ + iterator->second)->is_dirty_ = false;
      replacer_->Pin(iterator->second);
      free_list_.push_back(iterator->second);
      flag = true;
    }
  }
  return flag;
}

void BufferPoolManager::FlushAllPagesImpl() {
  lock_guard<mutex> lock{latch_};

  for (size_t i = 0; i < pool_size_; i++) {
    if ((pages_ + i)->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage((pages_ + i)->page_id_, (pages_ + i)->data_);
    }
  }
}

}  // namespace bustub
