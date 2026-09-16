// M5.1 实现（#2 TDD 阶段：全部为计数桩，使用例先 RED）。
#include "raft/metrics.h"

namespace raftkv::raft {

void Metrics::onWriteCompleted(uint64_t /*latencyUs*/) {}
void Metrics::onFsync(uint64_t /*durationUs*/) {}
void Metrics::onBatch(size_t /*entries*/) {}
void Metrics::onLockWait(uint64_t /*waitUs*/) {}
void Metrics::onElection() {}
void Metrics::onSnapshot(uint64_t /*bytes*/) {}
void Metrics::onConfigChange() {}
void Metrics::setReplicationLag(Index /*lag*/) {}
void Metrics::setInflightRpc(size_t /*n*/) {}

std::string Metrics::statusFragment() const { return std::string(); }
std::string Metrics::prometheusText() const { return std::string(); }

uint64_t Metrics::fsyncCalls() const { return 0; }
uint64_t Metrics::fsyncUs() const { return 0; }
uint64_t Metrics::writes() const { return 0; }
uint64_t Metrics::batches() const { return 0; }
uint64_t Metrics::batchEntries() const { return 0; }
uint64_t Metrics::elections() const { return 0; }
uint64_t Metrics::snapshots() const { return 0; }
uint64_t Metrics::snapshotBytes() const { return 0; }
uint64_t Metrics::configChanges() const { return 0; }
uint64_t Metrics::lockWaitUsTotal() const { return 0; }
uint64_t Metrics::lockWaitUsMax() const { return 0; }
uint64_t Metrics::latencyP50Us() const { return 0; }
uint64_t Metrics::latencyP99Us() const { return 0; }
uint64_t Metrics::qps() const { return 0; }

}  // namespace raftkv::raft
