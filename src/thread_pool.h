#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace raftkv {

// A small fixed-size thread pool running fire-and-forget void() jobs.
// Stage-1 building block; a lock-free MPSC queue replaces this later.
class ThreadPool {
 public:
  explicit ThreadPool(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      workers_.emplace_back([this] { workerLoop(); });
    }
  }

  ~ThreadPool() { stop(); }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stopping_) return;  // drop tasks requested after shutdown
      tasks_.push(std::move(task));
    }
    cv_.notify_one();
  }

  void stop() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      if (t.joinable()) t.join();
    }
  }

 private:
  void workerLoop() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (stopping_ && tasks_.empty()) return;
        task = std::move(tasks_.front());
        tasks_.pop();
      }
      task();
    }
  }

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stopping_ = false;
};

}  // namespace raftkv
