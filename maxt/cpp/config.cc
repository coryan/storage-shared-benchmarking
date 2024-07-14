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
#include <cstdint>
#include <string>
#include <vector>

export module maxt:config;

namespace maxt_internal {

struct config {
  std::string bucket_name;
  std::string deployment;
  std::string instance;
  std::string region;
  int iterations;
  std::vector<std::int64_t> object_sizes;
  std::vector<int> read_worker_counts;
  std::vector<int> write_worker_counts;
  std::vector<int> object_counts;
  std::vector<int> repeated_read_counts;
  std::string ssb_version;
  std::string sdk_version;
  std::string grpc_version;
  std::string protobuf_version;
  std::string http_client_version;
};

struct iteration_config {
  std::string experiment;
  std::int64_t object_size;
  int object_count;
  int worker_count;
  int repeated_read_count;

  auto operator<=>(iteration_config const&) const = default;
};

struct object_metadata {
  std::string bucket_name;
  std::string object_name;
  std::int64_t generation;
};

}  // namespace maxt_internal
