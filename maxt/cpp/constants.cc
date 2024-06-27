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
#include <string_view>

export module maxt:constants;

using namespace std::literals;

export auto constexpr kKB = 1'000;
export auto constexpr kMB = kKB * kKB;
export auto constexpr kKiB = 1024;
export auto constexpr kMiB = kKiB * kKiB;
export auto constexpr kJson = "JSON"sv;
export auto constexpr kGrpcCfe = "GRPC+CFE"sv;
export auto constexpr kGrpcDp = "GRPC+DP"sv;
export auto constexpr kAsyncGrpcCfe = "ASYNC+GRPC+CFE"sv;
export auto constexpr kAsyncGrpcDp = "ASYNC+GRPC+DP"sv;

export auto constexpr kAppName = "maxt"sv;
export auto constexpr kVersion = "0.0.0";
export auto constexpr kSchema = "https://opentelemetry.io/schemas/1.2.0";

export auto constexpr kLatencyHistogramName = "ssb/maxt/latency";
export auto constexpr kLatencyDescription =
    "Operation latency as measured by the benchmark.";
export auto constexpr kLatencyHistogramUnit = "s";

export auto constexpr kThroughputHistogramName = "ssb/maxt/throughput";
export auto constexpr kThroughputDescription =
    "Aggregate throughput latency as measured by the benchmark.";
export auto constexpr kThroughputHistogramUnit = "b/s";

export auto constexpr kCpuHistogramName = "ssb/maxt/cpu";
export auto constexpr kCpuDescription =
    "CPU usage per byte as measured by the benchmark.";
export auto constexpr kCpuHistogramUnit = "ns/B{CPU}";

export auto constexpr kMemoryHistogramName = "ssb/maxt/memory";
export auto constexpr kMemoryDescription =
    "Memory usage per byte as measured by the benchmark.";
export auto constexpr kMemoryHistogramUnit = "1{memory}";
