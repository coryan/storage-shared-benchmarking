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
#include <google/cloud/storage/client.h>
#include <opentelemetry/trace/provider.h>
#include <cstdint>
#include <future>
#include <memory>

export module maxt:sync_experiment;
import :constants;
import :config;
import :experiment;
import :resource_usage;

namespace {
namespace gc = ::google::cloud;
}

namespace maxt_internal {

auto upload_objects(
    config const& cfg, iteration_config const& iteration,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage::Client client, int task_id,
    std::shared_ptr<work_queue<std::string>> object_names, random_data data) {
  auto tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      otel_sv(kAppName));
  auto task_span = tracer->StartSpan(
      std::format("ssb::maxt::upload/{}", task_id),
      opentelemetry::common::MakeAttributes(
          make_common_attributes(cfg, iteration, "UPLOAD")),
      opentelemetry::trace::StartSpanOptions{.parent = span->GetContext()});
  std::vector<object_metadata> objects;
  auto i = 0;
  while (auto name = object_names->next()) {
    auto upload_span = tracer->StartSpan(
        std::format("ssb::maxt::upload/{}/{}", task_id, i++),
        opentelemetry::common::MakeAttributes(
            make_common_attributes(cfg, iteration, "UPLOAD")));
    auto const upload_scope = tracer->WithActiveSpan(upload_span);

    auto t = usage();
    auto os = client.WriteObject(cfg.bucket_name, *name);
    for (std::int64_t offset = 0; offset < iteration.object_size;) {
      auto n =
          std::min<std::int64_t>(data->size(), iteration.object_size - offset);
      os.write(data->data(), n);
      offset += n;
    }
    os.Close();
    t.record_single(cfg, iteration, "UPLOAD");
    auto meta = os.metadata();
    if (os.bad() || !meta) continue;
    objects.push_back({meta->bucket(), meta->name(), meta->generation()});
  }
  return objects;
}

auto download_objects(
    config const& cfg, iteration_config const& iteration,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage::Client client, int task_id,
    std::shared_ptr<work_queue<object_metadata>> objects) {
  auto tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      otel_sv(kAppName));
  auto task_span = tracer->StartSpan(
      std::format("ssb::maxt::download/{}", task_id),
      opentelemetry::common::MakeAttributes(
          make_common_attributes(cfg, iteration, "DOWNLOAD")),
      opentelemetry::trace::StartSpanOptions{.parent = span->GetContext()});
  auto total_bytes = std::int64_t{0};
  auto i = 0;
  std::vector<char> buffer(16 * kMiB);
  while (auto meta = objects->next()) {
    auto download_span = tracer->StartSpan(
        std::format("ssb::maxt::download/{}/{}", task_id, i++),
        opentelemetry::common::MakeAttributes(
            make_common_attributes(cfg, iteration, "DOWNLOAD")));
    auto const download_scope = tracer->WithActiveSpan(download_span);
    auto t = usage();
    auto is = client.ReadObject(meta->bucket_name, meta->object_name,
                                gc::storage::Generation(meta->generation));
    while (!is.bad() && !is.eof()) {
      is.read(buffer.data(), buffer.size());
      total_bytes += is.gcount();
    }
    t.record_single(cfg, iteration, "DOWNLOAD");
  }
  return total_bytes;
}

void delete_objects(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage::Client client, int task_count, int task_id,
    std::vector<object_metadata> const& objects) {
  auto const scope = opentelemetry::trace::Scope(span);
  for (std::size_t i = 0; i != objects.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& meta = objects[i];
    (void)client.DeleteObject(meta.bucket_name, meta.object_name,
                              gc::storage::Generation(meta.generation));
  }
}

class sync_experiment : public experiment {
 public:
  explicit sync_experiment(gc::storage::Client c) : client_(std::move(c)) {}

  std::vector<object_metadata> upload(std::mt19937_64& generator,
                                      config const& cfg,
                                      iteration_config const& iteration,
                                      random_data data) override {
    auto tracer =
        opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
            otel_sv(kAppName));
    auto span = tracer->GetCurrentSpan();
    auto queue =
        make_work_queue(generate_names(generator, iteration.object_count));

    std::vector<std::future<std::vector<object_metadata>>> tasks(
        iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, upload_objects, std::cref(cfg),
                        std::cref(iteration), span, client_, i++, queue, data);
    });
    std::vector<object_metadata> result;
    for (auto& t : tasks) try {
        auto m = t.get();
        result.insert(result.end(), std::move_iterator(m.begin()),
                      std::move_iterator(m.end()));
      } catch (...) { /* ignore task exceptions */
      }
    return result;
  }

  std::int64_t download(config const& cfg, iteration_config const& iteration,
                        std::vector<object_metadata> objects) override {
    auto tracer =
        opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
            otel_sv(kAppName));
    auto span = tracer->GetCurrentSpan();
    auto queue = make_work_queue(std::move(objects));

    std::vector<std::future<std::int64_t>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, download_objects, std::cref(cfg),
                        std::cref(iteration), span, client_, i++, queue);
    });
    auto result = std::int64_t{0};
    for (auto& t : tasks) try {
        result += t.get();
      } catch (...) { /* ignore task exceptions */
      }
    return result;
  }

  void cleanup(config const& cfg, iteration_config const& iteration,
               std::vector<object_metadata> objects) override {
    auto tracer =
        opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
            otel_sv(kAppName));
    auto span = tracer->GetCurrentSpan();

    std::vector<std::future<void>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, delete_objects, span, client_,
                        iteration.worker_count, i++, std::cref(objects));
    });
    for (auto& t : tasks) try {
        t.get();
      } catch (...) { /* ignore task exceptions */
      }
  }

 private:
  gc::storage::Client client_;
};

}  // namespace maxt_internal
