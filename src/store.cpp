#include "store.h"

#include <filesystem>
#include <stdexcept>
#include <utility>

#include "common.h"
#include "wal.h"

namespace raftkv {

Store::Store(std::string dataDir, bool sync) : dataDir_(std::move(dataDir)) {
  std::error_code ec;
  std::filesystem::create_directories(dataDir_, ec);
  if (ec) {
    throw std::runtime_error("cannot create data dir " + dataDir_ + ": " +
                             ec.message());
  }

  wal_ = std::make_unique<Wal>(dataDir_, sync);
  wal_->open([this](uint8_t op, const std::string& key,
                    const std::string& value) {
    if (op == static_cast<uint8_t>(OpCode::kPut)) {
      data_[key] = value;
    } else if (op == static_cast<uint8_t>(OpCode::kDel)) {
      data_.erase(key);
    }
  });
}

Store::~Store() = default;

StoreResult Store::put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!wal_->append(static_cast<uint8_t>(OpCode::kPut), key, value)) {
    return {false, false, "", "wal append failed"};
  }
  data_[key] = value;
  return {true, true, value, ""};
}

StoreResult Store::get(const std::string& key) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = data_.find(key);
  if (it == data_.end()) return {true, false, "", ""};
  return {true, true, it->second, ""};
}

StoreResult Store::del(const std::string& key) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!wal_->append(static_cast<uint8_t>(OpCode::kDel), key, "")) {
    return {false, false, "", "wal append failed"};
  }
  data_.erase(key);
  return {true, false, "", ""};
}

size_t Store::size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return data_.size();
}

}  // namespace raftkv
