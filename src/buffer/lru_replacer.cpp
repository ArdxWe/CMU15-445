//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock{mutex_};
  if (list_.empty()) {
    return false;
  }
  *frame_id = list_.back();
  list_.pop_back();
  map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock{mutex_};

  auto iterator = map_.find(frame_id);
  if (iterator != map_.end()) {
    list_.erase(iterator->second);
    map_.erase(iterator->first);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock{mutex_};

  auto iterator = map_.find(frame_id);
  if (iterator == map_.end()) {
    list_.push_front(frame_id);
    map_[frame_id] = list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock{mutex_};
  return map_.size();
}

}  // namespace bustub
