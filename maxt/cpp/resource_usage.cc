// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module;
#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/context/context.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>
#include <sys/resource.h>

export module maxt:resource_usage;
import :constants;
import :config;
import :monitoring;

namespace maxt_internal {

// We instrument `operator new` to track the number of allocated bytes. This
// global is used to track the value.
std::atomic<std::uint64_t> allocated_bytes{0};


auto bps(std::int64_t bytes, dseconds elapsed) {
  if (std::abs(elapsed.count()) < std::numeric_limits<double>::epsilon()) {
    return static_cast<double>(0.0);
  }
  return static_cast<double>(bytes * 8) / elapsed.count();
}

class usage {
 public:
  usage()
      : mem_(mem_now()),
        clock_(std::chrono::steady_clock::now()),
        cpu_(cpu_now()) {}

  auto elapsed_seconds() const {
    return std::chrono::duration_cast<dseconds>(
        std::chrono::steady_clock::now() - clock_);
  }

  void record(metrics const& mts, config const& cfg,
              iteration_config const& iteration, std::int64_t bytes,
              std::string_view op) const {
    auto const cpu_usage = cpu_now() - cpu_;
    auto const elapsed = elapsed_seconds();
    auto const mem_usage = mem_now() - mem_;

    auto per_byte = [bytes](auto value) {
      if (bytes == 0) return static_cast<double>(value);
      return static_cast<double>(value) / static_cast<double>(bytes);
    };

    mts.measurements->store(
        iteration, op,
        measurement_value{.throughput = bps(bytes, elapsed),
                           .cpu = per_byte(cpu_usage.count()),
                           .memory = per_byte(mem_usage)});
  }

  void record_single(metrics const& mts, config const& cfg,
                     iteration_config const& iteration,
                     std::string_view op) const {
    auto const elapsed = elapsed_seconds();
    mts.latency->Record(elapsed.count(),
                        opentelemetry::common::MakeAttributes(
                            make_common_attributes(cfg, iteration, op)),
                        opentelemetry::context::Context{});
  }

 private:
  static std::uint64_t mem_now() { return allocated_bytes.load(); }

  static auto as_nanoseconds(struct timeval const& t) {
    using ns = std::chrono::nanoseconds;
    return ns(std::chrono::seconds(t.tv_sec)) +
           ns(std::chrono::microseconds(t.tv_usec));
  }

  static std::chrono::nanoseconds cpu_now() {
    struct rusage ru {};
    (void)getrusage(RUSAGE_SELF, &ru);
    return as_nanoseconds(ru.ru_utime) + as_nanoseconds(ru.ru_stime);
  }

  std::uint64_t mem_;
  std::chrono::steady_clock::time_point clock_;
  std::chrono::nanoseconds cpu_;
};

}  // namespace maxt_internal
