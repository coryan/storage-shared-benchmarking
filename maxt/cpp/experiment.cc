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
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <vector>

export module maxt:experiment;
import :config;
import :monitoring;

namespace maxt_internal {

using random_data = std::shared_ptr<std::vector<char> const>;

struct experiment {
  virtual ~experiment() = default;
  virtual std::vector<object_metadata> upload(std::mt19937_64& generator,
                                              metrics const& mts,
                                              config const& cfg,
                                              iteration_config const& iteration,
                                              random_data data) = 0;
  virtual std::int64_t download(metrics const& mts, config const& cfg,
                                iteration_config const& iteration,
                                std::vector<object_metadata> objects) = 0;
  virtual void cleanup(config const& cfg, iteration_config const& iteration,
                       std::vector<object_metadata> objects) = 0;
};

using named_experiments = std::map<std::string, std::shared_ptr<experiment>>;

template <typename T>
class work_queue {
 public:
  work_queue() = default;
  explicit work_queue(std::vector<T> items) : items_(std::move(items)) {}

  std::optional<T> next() {
    std::lock_guard lk(mu_);
    if (items_.empty()) return std::nullopt;
    auto v = std::move(items_.back());
    items_.pop_back();
    return v;
  }

 private:
  mutable std::mutex mu_;
  std::vector<T> items_;
};

template <typename T>
auto make_work_queue(std::vector<T> items) {
  return std::make_shared<work_queue<T>>(std::move(items));
}

auto generate_uuid(std::mt19937_64& gen) {
  using uuid_generator = boost::uuids::basic_random_generator<std::mt19937_64>;
  return boost::uuids::to_string(uuid_generator{gen}());
}

auto make_object_name(std::mt19937_64& generator) {
  return generate_uuid(generator);
}

auto generate_names(std::mt19937_64& generator, std::size_t object_count) {
  std::vector<std::string> names;
  std::generate_n(std::back_inserter(names), object_count,
                  [&] { return make_object_name(generator); });
  return names;
}

}  // namespace maxt_internal
