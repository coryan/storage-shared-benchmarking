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
#include <google/cloud/storage/async/client.h>
#include <opentelemetry/trace/provider.h>
#include <memory>

export module maxt:async_experiment;
import :constants;
import :config;
import :experiment;
import :resource_usage;

namespace {
namespace gc = ::google::cloud;
}

namespace maxt_internal {

gc::future<std::vector<object_metadata>> async_upload_objects(
    metrics const& mts, config const& cfg, iteration_config const& iteration,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage_experimental::AsyncClient client, int task_id,
    std::shared_ptr<work_queue<std::string>> object_names, random_data data) {
  auto tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      otel_sv(kAppName));
  auto task_span = tracer->StartSpan(
      std::format("ssb::maxt::upload/{}", task_id),
      opentelemetry::common::MakeAttributes(
          make_common_attributes(cfg, iteration, "UPLOAD")),
      opentelemetry::trace::StartSpanOptions{.parent = span->GetContext()});
  gc::storage_experimental::BucketName bucket_name(cfg.bucket_name);
  std::vector<object_metadata> objects;
  auto i = 0;
  while (auto object_name = object_names->next()) {
    auto upload_span = tracer->StartSpan(
        std::format("ssb::maxt::upload/{}/{}", task_id, i++),
        opentelemetry::common::MakeAttributes(
            make_common_attributes(cfg, iteration, "UPLOAD")));
    auto const upload_scope = tracer->WithActiveSpan(upload_span);
    auto t = usage();
    auto [writer, token] =
        (co_await client.StartUnbufferedUpload(bucket_name, *object_name))
            .value();
    auto offset = std::int64_t{0};
    while (offset < iteration.object_size) {
      auto n =
          std::min<std::size_t>(iteration.object_size - offset, data->size());
      auto const scope = tracer->WithActiveSpan(upload_span);
      token = (co_await writer.Write(std::move(token),
                                     gc::storage_experimental::WritePayload(
                                         std::string(data->data(), n))))
                  .value();
      offset += n;
    }
    auto const scope = tracer->WithActiveSpan(upload_span);
    auto m = (co_await writer.Finalize(std::move(token))).value();
    t.record_single(mts, cfg, iteration, "UPLOAD");
    objects.push_back({m.bucket(), m.name(), m.generation()});
  }
  co_return objects;
}

gc::future<std::int64_t> async_download_objects(
    metrics const& mts, config const& cfg, iteration_config const& iteration,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage_experimental::AsyncClient client, int task_id,
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
  while (auto object = objects->next()) {
    auto download_span = tracer->StartSpan(
        std::format("ssb::maxt::download/{}/{}", task_id, i++),
        opentelemetry::common::MakeAttributes(
            make_common_attributes(cfg, iteration, "DOWNLOAD")));
    auto const download_scope = tracer->WithActiveSpan(download_span);
    auto t = usage();
    auto request = google::storage::v2::ReadObjectRequest{};
    request.set_bucket(object->bucket_name);
    request.set_object(object->object_name);
    request.set_generation(object->generation);
    auto [reader, token] =
        (co_await client.ReadObject(std::move(request))).value();
    while (token.valid()) {
      auto const scope = tracer->WithActiveSpan(download_span);
      auto [payload, t] = (co_await reader.Read(std::move(token))).value();
      for (auto sv : payload.contents()) total_bytes += sv.size();
      token = std::move(t);
    }
    t.record_single(mts, cfg, iteration, "DOWNLOAD");
  }
  co_return total_bytes;
}

gc::future<void> async_delete_objects(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span,
    gc::storage_experimental::AsyncClient client, int task_count, int task_id,
    std::vector<object_metadata> const& objects) {
  for (std::size_t i = 0; i != objects.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& object = objects[i];
    auto scope = opentelemetry::trace::Scope(span);
    auto request = google::storage::v2::DeleteObjectRequest{};
    request.set_bucket(object.bucket_name);
    request.set_object(object.object_name);
    request.set_generation(object.generation);
    (void)co_await client.DeleteObject(std::move(request));
  }
  co_return;
}

class async_experiment : public experiment {
 public:
  explicit async_experiment(gc::storage_experimental::AsyncClient c)
      : client_(std::move(c)) {}

  std::vector<object_metadata> upload(std::mt19937_64& generator,
                                      metrics const& mts, config const& cfg,
                                      iteration_config const& iteration,
                                      random_data data) override {
    auto tracer =
        opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
            otel_sv(kAppName));
    auto span = tracer->GetCurrentSpan();

    auto queue =
        make_work_queue(generate_names(generator, iteration.object_count));

    std::vector<gc::future<std::vector<object_metadata>>> tasks(
        iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      // Set the span before starting a coroutine. Otherwise we nest the span
      // with the suspend/resume callbacks for the coroutine.
      auto const scope = tracer->WithActiveSpan(span);
      return async_upload_objects(mts, cfg, iteration, span, client_, i++,
                                  queue, data);
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

  std::int64_t download(metrics const& mts, config const& cfg,
                        iteration_config const& iteration,
                        std::vector<object_metadata> objects) override {
    auto tracer =
        opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
            otel_sv(kAppName));
    auto span = tracer->GetCurrentSpan();

    auto queue = make_work_queue(std::move(objects));

    std::vector<gc::future<std::int64_t>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      // Set the span before starting a coroutine. Otherwise we nest the span
      // with the suspend/resume callbacks for the coroutine.
      auto const scope = tracer->WithActiveSpan(span);
      return async_download_objects(mts, cfg, iteration, span, client_, i++,
                                    queue);
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

    std::vector<gc::future<void>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      // Set the span before starting a coroutine. Otherwise we nest the span
      // with the suspend/resume callbacks for the coroutine.
      auto const scope = tracer->WithActiveSpan(span);
      return async_delete_objects(span, client_, iteration.worker_count, i++,
                                  std::cref(objects));
    });
    for (auto& t : tasks) try {
        t.get();
      } catch (...) { /* ignore task exceptions */
      }
  }

 private:
  gc::storage_experimental::AsyncClient client_;
};

}  // namespace maxt_internal
