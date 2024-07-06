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
#include <google/cloud/grpc_options.h>
#include <google/cloud/opentelemetry/monitoring_exporter.h>
#include <google/cloud/opentelemetry/resource_detector.h>
#include <google/cloud/opentelemetry/trace_exporter.h>
#include <google/cloud/opentelemetry_options.h>
#include <google/cloud/project.h>
#include <google/cloud/storage/async/client.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/grpc_plugin.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/version.h>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <curl/curlver.h>
#include <grpcpp/grpcpp.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#include <opentelemetry/sdk/metrics/view/view_registry.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/samplers/trace_id_ratio_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
#include <map>
#include <new>
#include <numeric>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <sys/resource.h>

export module maxt;
import :constants;
import :parse_args;

namespace {

using namespace std::literals;
namespace gc = ::google::cloud;

auto constexpr kRandomDataSize = 32 * kMiB;

auto constexpr kLatencyHistogramName = "ssb/maxt/latency";
auto constexpr kLatencyDescription =
    "Operation latency as measured by the benchmark.";
auto constexpr kLatencyHistogramUnit = "s";

auto constexpr kThroughputHistogramName = "ssb/maxt/throughput";
auto constexpr kThroughputDescription =
    "Aggregate throughput latency as measured by the benchmark.";
auto constexpr kThroughputHistogramUnit = "b/s";

auto constexpr kCpuHistogramName = "ssb/maxt/cpu";
auto constexpr kCpuDescription =
    "CPU usage per byte as measured by the benchmark.";
auto constexpr kCpuHistogramUnit = "ns/B{CPU}";

auto constexpr kMemoryHistogramName = "ssb/maxt/memory";
auto constexpr kMemoryDescription =
    "Memory usage per byte as measured by the benchmark.";
auto constexpr kMemoryHistogramUnit = "1{memory}";

auto constexpr kVersion = "1.2.0";
auto constexpr kSchema = "https://opentelemetry.io/schemas/1.2.0";

}  // namespace

namespace maxt_internal {

using dseconds = std::chrono::duration<double, std::ratio<1>>;

using histogram_ptr =
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>;

struct config {
  std::string bucket_name;
  std::string deployment;
  std::string instance;
  std::string region;
  int iterations;
  std::vector<std::int64_t> object_sizes;
  std::vector<int> worker_counts;
  std::vector<int> object_counts;
  std::vector<int> repeated_read_counts;
  std::string ssb_version;
  std::string sdk_version;
  std::string grpc_version;
  std::string protobuf_version;
  std::string http_client_version;
  histogram_ptr latency;
  histogram_ptr throughput;
  histogram_ptr cpu;
  histogram_ptr memory;
};

using random_data = std::shared_ptr<std::vector<char> const>;

struct object_metadata {
  std::string bucket_name;
  std::string object_name;
  std::int64_t generation;
};

struct iteration_config {
  std::string experiment;
  std::int64_t object_size;
  int object_count;
  int worker_count;
  int repeated_read_count;
};

struct experiment {
  virtual ~experiment() = default;
  virtual std::vector<object_metadata> upload(std::mt19937_64& generator,
                                              config const& cfg,
                                              iteration_config const& iteration,
                                              random_data data) = 0;
  virtual std::int64_t download(config const& cfg,
                                iteration_config const& iteration,
                                std::vector<object_metadata> objects) = 0;
  virtual void cleanup(config const& cfg, iteration_config const& iteration,
                       std::vector<object_metadata> objects) = 0;
};

using named_experiments = std::map<std::string, std::shared_ptr<experiment>>;
named_experiments make_experiments(
    boost::program_options::variables_map const& vm);

std::string discover_region();

std::unique_ptr<opentelemetry::metrics::MeterProvider> make_meter_provider(
    google::cloud::Project const& project, std::string const& instance);

std::shared_ptr<opentelemetry::trace::TracerProvider> make_tracer_provider(
    boost::program_options::variables_map const& vm) {
  auto ratio = vm["tracing-rate"].as<double>();
  if (ratio == 0.0) return {};
  auto project = google::cloud::Project(vm["project-id"].as<std::string>());
  auto detector = google::cloud::otel::MakeResourceDetector();
  auto processor =
      std::make_unique<opentelemetry::sdk::trace::BatchSpanProcessor>(
          google::cloud::otel::MakeTraceExporter(std::move(project),
                                                 google::cloud::Options{}),
          opentelemetry::sdk::trace::BatchSpanProcessorOptions{
              .max_queue_size = vm["max-queue-size"].as<std::size_t>()});
  auto provider = std::make_shared<opentelemetry::sdk::trace::TracerProvider>(
      std::move(processor), detector->Detect(),
      opentelemetry::sdk::trace::TraceIdRatioBasedSamplerFactory::Create(
          ratio));
  opentelemetry::trace::Provider::SetTracerProvider(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
          provider));
  return provider;
}

auto make_prng_bits_generator() {
  // Random number initialization in C++ is more tedious than it should be.
  // First you get some entropy from the random device. We don't need too much,
  // just a word will do:
  auto entropy = std::vector<unsigned int>({std::random_device{}()});
  // Then you shuffle these bits. The recommended PRNG has poor behavior if
  // too many of the seed bits are all zeroes. This shuffling step avoids that
  // problem.
  auto seq = std::seed_seq(entropy.begin(), entropy.end());
  // Now initialize the PRNG.
  return std::mt19937_64(seq);
}

auto generate_uuid(std::mt19937_64& gen) {
  using uuid_generator = boost::uuids::basic_random_generator<std::mt19937_64>;
  return boost::uuids::to_string(uuid_generator{gen}());
}

template <typename Collection>
auto pick_one(std::mt19937_64& generator, Collection const& collection) {
  auto index = std::uniform_int_distribution<std::size_t>(
      0, std::size(collection) - 1)(generator);
  return *std::next(std::begin(collection), index);
}

auto make_object_name(std::mt19937_64& generator) {
  return generate_uuid(generator);
}

// We instrument `operator new` to track the number of allocated bytes. This
// global is used to track the value.
std::atomic<std::uint64_t> allocated_bytes{0};

auto bps(std::int64_t bytes, dseconds elapsed) {
  if (std::abs(elapsed.count()) < std::numeric_limits<double>::epsilon()) {
    return static_cast<double>(0.0);
  }
  return static_cast<double>(bytes * 8) / elapsed.count();
}

auto otel_sv(std::string_view s) {
  return opentelemetry::nostd::string_view(s.data(), s.size());
}

auto make_common_attributes(config const& cfg,
                            iteration_config const& iteration,
                            std::string_view op) {
  return std::vector<std::pair<opentelemetry::nostd::string_view,
                               opentelemetry::common::AttributeValue>>{
      {"ssb.app", otel_sv(kAppName)},
      {"ssb.op", otel_sv(op)},
      {"ssb.language", "cpp"},
      {"ssb.experiment", iteration.experiment},
      {"ssb.object-size", iteration.object_size},
      {"ssb.object-count", iteration.object_count},
      {"ssb.worker-count", iteration.worker_count},
      {"ssb.repeated-read-count", iteration.repeated_read_count},
      {"ssb.deployment", cfg.deployment},
      {"ssb.instance", cfg.instance},
      {"ssb.region", cfg.region},
      {"ssb.version", cfg.ssb_version},
      {"ssb.version.sdk", cfg.sdk_version},
      {"ssb.version.grpc", cfg.grpc_version},
      {"ssb.version.protobuf", cfg.protobuf_version},
      {"ssb.version.http-client", cfg.http_client_version},
  };
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

  void record(config const& cfg, iteration_config const& iteration,
              std::int64_t bytes, auto span, auto attributes) const {
    auto const cpu_usage = cpu_now() - cpu_;
    auto const elapsed = elapsed_seconds();
    auto const mem_usage = mem_now() - mem_;

    auto per_byte = [bytes](auto value) {
      if (bytes == 0) return static_cast<double>(value);
      return static_cast<double>(value) / static_cast<double>(bytes);
    };

    cfg.throughput->Record(
        bps(bytes, elapsed), attributes,
        opentelemetry::context::Context{}.SetValue("span", span));
    cfg.cpu->Record(per_byte(cpu_usage.count()), attributes,
                    opentelemetry::context::Context{}.SetValue("span", span));
    cfg.memory->Record(
        per_byte(mem_usage), attributes,
        opentelemetry::context::Context{}.SetValue("span", span));
    span->End();
  }

  void record_single(config const& cfg, iteration_config const& iteration,
                     std::string_view op) const {
    auto const elapsed = elapsed_seconds();
    cfg.latency->Record(elapsed.count(),
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

auto generate_names(std::mt19937_64& generator, std::size_t object_count) {
  std::vector<std::string> names;
  std::generate_n(std::back_inserter(names), object_count,
                  [&] { return make_object_name(generator); });
  return names;
}

auto generate_random_data(std::mt19937_64& generator) {
  std::vector<char> data(kRandomDataSize);
  std::generate(data.begin(), data.end(), [&generator] {
    return std::uniform_int_distribution<char>(
        std::numeric_limits<char>::min(),
        std::numeric_limits<char>::max())(generator);
  });
  return data;
}

void run(config cfg, named_experiments experiments) {
  // Obtain a tracer for the Shared Storage Benchmarks. We create traces that
  // logically connect the client library traces for uploads and downloads.
  auto tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      otel_sv(kAppName));

  auto generator = make_prng_bits_generator();
  auto data = std::make_shared<std::vector<char> const>(
      generate_random_data(generator));

  for (int i = 0; i != cfg.iterations; ++i) {
    auto const [experiment, runner] = pick_one(generator, experiments);

    auto const iteration = iteration_config{
        .experiment = std::move(experiment),
        .object_size = pick_one(generator, cfg.object_sizes),
        .object_count = pick_one(generator, cfg.object_counts),
        .worker_count = pick_one(generator, cfg.worker_counts),
        .repeated_read_count = pick_one(generator, cfg.repeated_read_counts),
    };

    auto as_attributes = [](auto const& attr) {
      using value_type = typename std::decay_t<decltype(attr)>::value_type;
      using span_t = opentelemetry::nostd::span<value_type const>;
      return opentelemetry::common::MakeAttributes(
          span_t(attr.data(), attr.size()));
    };
    // Run the upload step in its own scope
    auto const objects = [&] {
      auto attributes = make_common_attributes(cfg, iteration, "UPLOAD");
      auto span =
          tracer->StartSpan("ssb::maxt::upload",
                            opentelemetry::common::MakeAttributes(attributes));
      auto active = tracer->WithActiveSpan(span);
      auto const t = usage();
      auto objects = runner->upload(generator, cfg, iteration, data);
      auto const bytes = objects.size() * iteration.object_size;
      t.record(cfg, iteration, bytes, span, as_attributes(attributes));
      std::cout << "UPLOAD " << experiment
                << " Gbps: " << bps(bytes, t.elapsed_seconds()) / 1'000'000'000
                << std::endl;
      return objects;
    }();

    {
      auto const attributes =
          make_common_attributes(cfg, iteration, "DOWNLOAD");
      std::vector<object_metadata> repeated;
      repeated.reserve(iteration.repeated_read_count * objects.size());
      for (int i = 0; i != iteration.repeated_read_count; ++i) {
        repeated.insert(repeated.end(), objects.begin(), objects.end());
      }
      auto download_span =
          tracer->StartSpan("ssb::maxt::download",
                            opentelemetry::common::MakeAttributes(attributes));
      auto active = tracer->WithActiveSpan(download_span);
      auto const t = usage();
      auto const bytes = runner->download(cfg, iteration, repeated);
      t.record(cfg, iteration, bytes, download_span, as_attributes(attributes));
      std::cout << "DOWNLOAD " << experiment
                << " Gbps: " << bps(bytes, t.elapsed_seconds()) / 1'000'000'000
                << std::endl;
    }

    {
      auto const attributes = make_common_attributes(cfg, iteration, "CLEANUP");
      auto span =
          tracer->StartSpan("ssb::maxt::cleanup",
                            opentelemetry::common::MakeAttributes(attributes));
      auto active = tracer->WithActiveSpan(span);
      runner->cleanup(cfg, iteration, objects);
      span->End();
    }
  }
}

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

gc::future<std::vector<object_metadata>> async_upload_objects(
    config const& cfg, iteration_config const& iteration,
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
    t.record_single(cfg, iteration, "UPLOAD");
    objects.push_back({m.bucket(), m.name(), m.generation()});
  }
  co_return objects;
}

gc::future<std::int64_t> async_download_objects(
    config const& cfg, iteration_config const& iteration,
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
    t.record_single(cfg, iteration, "DOWNLOAD");
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
                                      config const& cfg,
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
      return async_upload_objects(cfg, iteration, span, client_, i++, queue,
                                  data);
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

    std::vector<gc::future<std::int64_t>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      // Set the span before starting a coroutine. Otherwise we nest the span
      // with the suspend/resume callbacks for the coroutine.
      auto const scope = tracer->WithActiveSpan(span);
      return async_download_objects(cfg, iteration, span, client_, i++, queue);
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

auto options(boost::program_options::variables_map const& vm) {
  using namespace std::chrono_literals;
  auto enable_tracing = vm["tracing-rate"].as<double>() != 0.0;
  return gc::Options{}
      .set<gc::storage::TransferStallMinimumRateOption>(10 * kMiB)
      .set<gc::storage::TransferStallTimeoutOption>(10s)
      .set<gc::storage::DownloadStallMinimumRateOption>(20 * kMiB)
      .set<gc::storage::DownloadStallTimeoutOption>(1s)
      .set<gc::storage::UploadBufferSizeOption>(256 * kKiB)
      .set<gc::storage_experimental::HttpVersionOption>("1.1")
      .set<gc::OpenTelemetryTracingOption>(enable_tracing);
}

auto make_json(boost::program_options::variables_map const& vm) {
  return gc::storage::Client(options(vm));
}

auto grpc_options(boost::program_options::variables_map const& vm,
                  std::string prefix, std::string_view endpoint) {
  auto opts = options(vm).set<gc::EndpointOption>(std::string(endpoint));
  auto const l = vm.find(prefix + "channels");
  if (l != vm.end()) {
    opts.set<gc::GrpcNumChannelsOption>(l->second.as<int>());
  }
  return opts;
}

auto make_grpc(boost::program_options::variables_map const& vm) {
  return gc::storage_experimental::DefaultGrpcClient(
      grpc_options(vm, "cfe-", "storage.googleapis.com"));
}

auto make_dp(boost::program_options::variables_map const& vm) {
  return gc::storage_experimental::DefaultGrpcClient(
      grpc_options(vm, "dp-", "google-c2p:///storage.googleapis.com"));
}

auto async_options(boost::program_options::variables_map const& vm,
                   std::string prefix, std::string_view endpoint) {
  auto opts = options(vm).set<gc::EndpointOption>(std::string(endpoint));
  auto l = vm.find(prefix + "thread-pool");
  if (l != vm.end()) {
    opts.set<gc::GrpcBackgroundThreadPoolSizeOption>(l->second.as<int>());
  }
  l = vm.find(prefix + "channels");
  if (l != vm.end()) {
    opts.set<gc::GrpcNumChannelsOption>(l->second.as<int>());
  }
  return opts;
}

auto make_async_cfe(boost::program_options::variables_map const& vm) {
  return gc::storage_experimental::AsyncClient(
      async_options(vm, "cfe-", "storage.googleapis.com"));
}

auto make_async_dp(boost::program_options::variables_map const& vm) {
  return gc::storage_experimental::AsyncClient(
      async_options(vm, "dp-", "google-c2p:///storage.googleapis.com"));
}

named_experiments make_experiments(
    boost::program_options::variables_map const& vm) {
  named_experiments ne;
  for (auto const& name : vm["experiments"].as<std::vector<std::string>>()) {
    if (name == kJson) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_json(vm)));
    } else if (name == kGrpcCfe) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_grpc(vm)));
    } else if (name == kGrpcDp) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_dp(vm)));
    } else if (name == kAsyncGrpcCfe) {
      ne.emplace(name, std::make_shared<async_experiment>(make_async_cfe(vm)));
    } else if (name == kAsyncGrpcDp) {
      ne.emplace(name, std::make_shared<async_experiment>(make_async_dp(vm)));
    } else {
      throw std::invalid_argument("Unknown experiment name: " + name);
    }
  }
  return ne;
}

std::string discover_region() {
  namespace sc = opentelemetry::sdk::resource::SemanticConventions;
  auto detector = google::cloud::otel::MakeResourceDetector();
  auto detected_resource = detector->Detect();
  for (auto const& [k, v] : detected_resource.GetAttributes()) {
    if (k == sc::kCloudRegion) return std::get<std::string>(v);
  }
  return std::string("unknown");
}

auto make_resource(std::string const& instance) {
  // Create an OTel resource that maps to `generic_task` on GCM.
  namespace sc = opentelemetry::sdk::resource::SemanticConventions;
  auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes();
  resource_attributes.SetAttribute(sc::kServiceNamespace, "default");
  resource_attributes.SetAttribute(sc::kServiceName, std::string(kAppName));
  resource_attributes.SetAttribute(sc::kServiceInstanceId, instance);

  auto detector = google::cloud::otel::MakeResourceDetector();
  auto detected_resource = detector->Detect();
  for (auto const& [k, v] : detected_resource.GetAttributes()) {
    if (k == sc::kCloudRegion) {
      resource_attributes.SetAttribute(k, std::get<std::string>(v));
    } else if (k == sc::kCloudAvailabilityZone) {
      resource_attributes.SetAttribute(k, std::get<std::string>(v));
    }
  }
  return opentelemetry::sdk::resource::Resource::Create(resource_attributes);
}

auto make_latency_histogram_boundaries() {
  using namespace std::chrono_literals;
  // Cloud Monitoring only supports up to 200 buckets per histogram, we have
  // to choose them carefully.
  std::vector<double> boundaries;
  auto boundary = 0ms;
  auto increment = 100ms;
  for (int i = 0; boundaries.size() != 200; ++i) {
    boundaries.push_back(
        std::chrono::duration_cast<dseconds>(boundary).count());
    if (i != 0 && i % 10 == 0) increment *= 2;
    boundary += increment;
  }
  return boundaries;
}

auto make_throughput_histogram_boundaries() {
  // Cloud Monitoring only supports up to 200 buckets per histogram, we have
  // to choose them carefully.
  std::vector<double> boundaries;
  // The units are bits/s, use 2 Gpbs intervals, as some VMs support 100 Gbps,
  // and that number can only go up.
  auto boundary = 0.0;
  auto increment = 2'000'000'000.0;
  for (int i = 0; i != 200; ++i) {
    boundaries.push_back(boundary);
    boundary += increment;
  }
  return boundaries;
}

auto make_cpu_histogram_boundaries() {
  // Cloud Monitoring only supports up to 200 buckets per histogram, we have
  // to choose them carefully.
  std::vector<double> boundaries;
  // The units are ns/B, we start with increments of 0.1ns.
  auto boundary = 0.0;
  auto increment = 1.0 / 8.0;
  for (int i = 0; i != 200; ++i) {
    boundaries.push_back(boundary);
    if (i != 0 && i % 32 == 0) increment *= 2;
    boundary += increment;
  }
  return boundaries;
}

auto make_memory_histogram_boundaries() {
  // Cloud Monitoring only supports up to 200 buckets per histogram, we have
  // to choose them carefully.
  std::vector<double> boundaries;
  // We expect the library to use less memory than the transferred size, that
  // is why we stream the data. Use exponentially growing bucket sizes, since
  // we have no better ideas.
  auto boundary = 0.0;
  auto increment = 1.0 / 16.0;
  for (int i = 0; i != 200; ++i) {
    boundaries.push_back(boundary);
    boundary += increment;
    if (i != 0 && i % 16 == 0) increment *= 2;
  }
  return boundaries;
}

void add_histogram_view(opentelemetry::sdk::metrics::MeterProvider& provider,
                        std::string const& name, std::string const& description,
                        std::string const& unit,
                        std::vector<double> boundaries) {
  auto histogram_instrument_selector =
      opentelemetry::sdk::metrics::InstrumentSelectorFactory::Create(
          opentelemetry::sdk::metrics::InstrumentType::kHistogram, name, unit);
  auto histogram_meter_selector =
      opentelemetry::sdk::metrics::MeterSelectorFactory::Create(
          std::string{kAppName}, kVersion, kSchema);

  auto histogram_aggregation_config = std::make_unique<
      opentelemetry::sdk::metrics::HistogramAggregationConfig>();
  histogram_aggregation_config->boundaries_ = std::move(boundaries);
  // Type-erase and convert to shared_ptr.
  auto aggregation_config =
      std::shared_ptr<opentelemetry::sdk::metrics::AggregationConfig>(
          std::move(histogram_aggregation_config));

  auto histogram_view = opentelemetry::sdk::metrics::ViewFactory::Create(
      name, description, unit,
      opentelemetry::sdk::metrics::AggregationType::kHistogram,
      aggregation_config);

  provider.AddView(std::move(histogram_instrument_selector),
                   std::move(histogram_meter_selector),
                   std::move(histogram_view));
}

std::unique_ptr<opentelemetry::metrics::MeterProvider> make_meter_provider(
    google::cloud::Project const& project, std::string const& instance) {
  // We want to configure the latency histogram buckets. Seemingly, this is
  // done rather indirectly in OpenTelemetry. One defines a "selector" that
  // matches the target histogram, and stores the configuration there.
  auto exporter = gc::otel_internal::MakeMonitoringExporter(
      project, gc::monitoring_v3::MakeMetricServiceConnection());

  auto reader_options =
      opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions{};
  reader_options.export_interval_millis = std::chrono::seconds(60);
  reader_options.export_timeout_millis = std::chrono::seconds(15);

  std::shared_ptr<opentelemetry::sdk::metrics::MetricReader> reader =
      opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
          std::move(exporter), reader_options);

  auto provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create(
      std::make_unique<opentelemetry::sdk::metrics::ViewRegistry>(),
      make_resource(instance));
  auto& p = dynamic_cast<opentelemetry::sdk::metrics::MeterProvider&>(
      *provider.get());
  p.AddMetricReader(reader);

  add_histogram_view(p, kLatencyHistogramName, kLatencyDescription,
                     kLatencyHistogramUnit,
                     make_latency_histogram_boundaries());
  add_histogram_view(p, kThroughputHistogramName, kThroughputDescription,
                     kThroughputHistogramUnit,
                     make_throughput_histogram_boundaries());
  add_histogram_view(p, kCpuHistogramName, kCpuDescription, kCpuHistogramUnit,
                     make_cpu_histogram_boundaries());
  add_histogram_view(p, kMemoryHistogramName, kMemoryDescription,
                     kMemoryHistogramUnit, make_memory_histogram_boundaries());

  return provider;
}

}  // namespace maxt_internal

export namespace maxt {

auto parse_args(int argc, char* argv[]) { return ::parse_args(argc, argv); }
void count_allocations(std::size_t count) {
  maxt_internal::allocated_bytes.fetch_add(count);
}
auto make_prng_bits_generator() {
  return maxt_internal::make_prng_bits_generator();
}
auto generate_uuid(auto& generator) {
  return maxt_internal::generate_uuid(generator);
}

auto make_config(boost::program_options::variables_map const& vm) {
  auto generator = maxt::make_prng_bits_generator();
  auto const instance = maxt::generate_uuid(generator);

  auto const bucket_name = vm["bucket"].as<std::string>();
  auto const deployment = vm["deployment"].as<std::string>();

  auto tracer_provider = maxt_internal::make_tracer_provider(vm);
  auto meter_provider = maxt_internal::make_meter_provider(
      google::cloud::Project(vm["project-id"].as<std::string>()), instance);
  auto meter =
      meter_provider->GetMeter(std::string{kAppName}, kVersion, kSchema);
  maxt_internal::histogram_ptr latency = meter->CreateDoubleHistogram(
      kLatencyHistogramName, kLatencyDescription, kLatencyHistogramUnit);
  maxt_internal::histogram_ptr throughput = meter->CreateDoubleHistogram(
      kThroughputHistogramName, kThroughputDescription,
      kThroughputHistogramUnit);
  maxt_internal::histogram_ptr cpu = meter->CreateDoubleHistogram(
      kCpuHistogramName, kCpuDescription, kCpuHistogramUnit);
  maxt_internal::histogram_ptr memory = meter->CreateDoubleHistogram(
      kMemoryHistogramName, kMemoryDescription, kMemoryHistogramUnit);

  return maxt_internal::config{
      .bucket_name = std::move(bucket_name),
      .deployment = deployment,
      .instance = instance,
      .region = maxt_internal::discover_region(),
      .iterations = vm["iterations"].as<int>(),
      .object_sizes = vm["object-sizes"].as<std::vector<std::int64_t>>(),
      .worker_counts = vm["worker-counts"].as<std::vector<int>>(),
      .object_counts = vm["object-counts"].as<std::vector<int>>(),
      .repeated_read_counts = vm["repeated-read-counts"].as<std::vector<int>>(),
      .ssb_version = SSB_VERSION,
      .sdk_version = gc::version_string(),
      .grpc_version = grpc::Version(),
      .protobuf_version = SSB_PROTOBUF_VERSION,
      .http_client_version = LIBCURL_VERSION,
      .latency = std::move(latency),
      .throughput = std::move(throughput),
      .cpu = std::move(cpu),
      .memory = std::move(memory),
  };
}

maxt_internal::named_experiments make_experiments(
    boost::program_options::variables_map const& vm) {
  return maxt_internal::make_experiments(vm);
}

void run(maxt_internal::config cfg,
         maxt_internal::named_experiments experiments) {
  return maxt_internal::run(std::move(cfg), std::move(experiments));
}

}  // namespace maxt
