// 微基准：本机 fsync / fdatasync / 预分配+fdatasync / O_DIRECT+fdatasync 的每次提交延迟
// 目的：判断 p=1 ≤8ms/写 的判据是否受"单次 fsync 就要 8ms"这一物理下限限制，
//       以及是否可以通过预分配（避免每次追加都触发 ext4 元数据日志）把提交延迟压下来。
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <algorithm>

static double nowMs() {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

static void bench(const char* name, const char* path, bool useFdatasync,
                  bool preallocate, bool direct) {
  int flags = O_RDWR | O_CREAT | O_TRUNC;
  if (direct) flags |= O_DIRECT;
  const int fd = ::open(path, flags, 0644);
  if (fd < 0) { std::printf("%-28s open failed\n", name); return; }
  const size_t kRec = 4096;
  void* buf = nullptr;
  if (posix_memalign(&buf, 4096, kRec) != 0) { ::close(fd); return; }
  std::memset(buf, 'x', kRec);
  if (preallocate) {
    // 预分配 64MiB：之后追加不再改变文件大小 -> fdatasync 无需提交元数据
    if (::posix_fallocate(fd, 0, 64u << 20) != 0) {
      std::printf("%-28s posix_fallocate failed (fallback truncate)\n", name);
      if (::ftruncate(fd, 64u << 20) != 0) { /* ignore */ }
    }
  }
  const int iters = 200;
  std::vector<double> lat;
  lat.reserve(iters);
  for (int i = 0; i < iters; ++i) {
    const double t0 = nowMs();
    ssize_t w = ::pwrite(fd, buf, kRec, static_cast<off_t>(i) * kRec);
    if (w != static_cast<ssize_t>(kRec)) { std::printf("%-28s write short\n", name); break; }
    const int rc = useFdatasync ? ::fdatasync(fd) : ::fsync(fd);
    if (rc != 0) { std::printf("%-28s flush failed errno=%d\n", name, errno); break; }
    lat.push_back(nowMs() - t0);
  }
  std::sort(lat.begin(), lat.end());
  double sum = 0;
  for (double v : lat) sum += v;
  if (!lat.empty()) {
    std::printf("%-28s n=%zu  avg=%.3fms  p50=%.3fms  p99=%.3fms  max=%.3fms\n", name,
                lat.size(), sum / lat.size(), lat[lat.size() / 2],
                lat[static_cast<size_t>(lat.size() * 0.99)], lat.back());
  }
  ::free(buf);
  ::close(fd);
  ::unlink(path);
}

int main() {
  std::printf("== 单次提交延迟（4096B 追加 + flush，200 次，本机 %s）==\n", "/tmp");
  bench("fsync（当前实现）", "/tmp/fsb_fsync.dat", false, false, false);
  bench("fdatasync", "/tmp/fsb_fdata.dat", true, false, false);
  bench("prealloc+fdatasync", "/tmp/fsb_pre.dat", true, true, false);
  bench("prealloc+fsync", "/tmp/fsb_pre2.dat", false, true, false);
  bench("O_DIRECT+fdatasync", "/tmp/fsb_od.dat", true, false, true);
  return 0;
}
