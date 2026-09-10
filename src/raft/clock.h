#pragma once

#include <chrono>
#include <cstdint>

namespace raftkv::raft {

class Clock {
 public:
  virtual ~Clock() = default;
  virtual uint64_t nowMs() const = 0;
};

// Wall clock for production (main_raft_node).
class SteadyClock : public Clock {
 public:
  uint64_t nowMs() const override {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  }
};

// Deterministic clock for unit tests (no real time, no flakiness).
class FakeClock : public Clock {
 public:
  uint64_t nowMs() const override { return now_; }
  void advance(uint64_t ms) { now_ += ms; }

 private:
  uint64_t now_ = 0;
};

}  // namespace raftkv::raft
