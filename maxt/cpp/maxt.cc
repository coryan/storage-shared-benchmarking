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

#include <google/cloud/grpc_options.h>
#include <google/cloud/internal/build_info.h>
#include <google/cloud/internal/compiler_info.h>
#include <google/cloud/opentelemetry/configure_basic_tracing.h>
#include <google/cloud/opentelemetry/monitoring_exporter.h>
#include <google/cloud/opentelemetry/resource_detector.h>
#include <google/cloud/opentelemetry_options.h>
#include <google/cloud/project.h>
#include <google/cloud/storage/async/client.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/grpc_plugin.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/version.h>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <curl/curlver.h>
#include <grpcpp/grpcpp.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/metrics/export/metric_producer.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
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

namespace {

using namespace std::literals;

auto constexpr kKB = 1'000;
auto constexpr kMB = kKB * kKB;
auto constexpr kKiB = 1024;
auto constexpr kMiB = kKiB * kKiB;
auto constexpr kRandomDataSize = 32 * kMiB;

auto constexpr kAppName = "maxt"sv;

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

auto constexpr kDefaultIterations = 1'000'000;
auto constexpr kDefaultSampleRate = 0.05;

namespace gc = ::google::cloud;
using dseconds = std::chrono::duration<double, std::ratio<1>>;

boost::program_options::variables_map parse_args(int argc, char* argv[]);

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
  std::int64_t object_size;
  int object_count;
  int worker_count;
  int repeated_read_count;
};

struct experiment {
  virtual std::vector<object_metadata> upload(
      std::mt19937_64& generator, config const& cfg, random_data data,
      iteration_config const& iteration) = 0;
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

void run(config cfg, named_experiments experiments);

}  // namespace

int main(int argc, char* argv[]) try {
  auto const vm = parse_args(argc, argv);

  auto const project = gc::Project(vm["project-id"].as<std::string>());
  auto generator = make_prng_bits_generator();
  auto const instance = generate_uuid(generator);

  auto const bucket_name = vm["bucket"].as<std::string>();
  auto const deployment = vm["deployment"].as<std::string>();
  auto const tracing_rate = vm["tracing-rate"].as<double>();

  auto join = [](auto collection) {
    if (collection.empty()) return std::string{};
    return std::accumulate(std::next(collection.begin()), collection.end(),
                           boost::lexical_cast<std::string>(collection.front()),
                           [](auto a, auto const& b) {
                             a += ",";
                             a += boost::lexical_cast<std::string>(b);
                             return a;
                           });
  };

  auto const tracing = gc::otel::ConfigureBasicTracing(
      project,
      gc::Options{}.set<gc::otel::BasicTracingRateOption>(tracing_rate));

  auto provider =
      make_meter_provider(google::cloud::Project(project), instance);

  // Create a histogram to capture the performance results.
  auto meter = provider->GetMeter(std::string{kAppName}, kVersion, kSchema);
  histogram_ptr latency = meter->CreateDoubleHistogram(
      kLatencyHistogramName, kLatencyDescription, kLatencyHistogramUnit);
  histogram_ptr throughput = meter->CreateDoubleHistogram(
      kThroughputHistogramName, kThroughputDescription,
      kThroughputHistogramUnit);
  histogram_ptr cpu = meter->CreateDoubleHistogram(
      kCpuHistogramName, kCpuDescription, kCpuHistogramUnit);
  histogram_ptr memory = meter->CreateDoubleHistogram(
      kMemoryHistogramName, kMemoryDescription, kMemoryHistogramUnit);

  auto cfg = config{
      .bucket_name = std::move(bucket_name),
      .deployment = deployment,
      .instance = instance,
      .region = discover_region(),
      .iterations = vm["iterations"].as<int>(),
      .object_sizes = vm["object-sizes"].as<std::vector<std::int64_t>>(),
      .worker_counts = vm["worker-counts"].as<std::vector<int>>(),
      .object_counts = vm["object-counts"].as<std::vector<int>>(),
      .repeated_read_counts = vm["repeated-read-counts"].as<std::vector<int>>(),
      .latency = std::move(latency),
      .throughput = std::move(throughput),
      .cpu = std::move(cpu),
      .memory = std::move(memory),
  };

  // Using the `internal` namespace is frowned upon. The C++ SDK team may change
  // the types and functions in this namespace at any time. If this ever breaks
  // we will find out at compile time, and will need to detect the compiler and
  // build flags ourselves.
  namespace gci = ::google::cloud::internal;
  std::cout << "## Starting continuous GCS C++ SDK benchmark"              //
            << "\n# bucket: " << cfg.bucket_name                           //
            << "\n# deployment: " << cfg.deployment                        //
            << "\n# instance: " << cfg.instance                            //
            << "\n# region: " << cfg.region                                //
            << "\n# iterations: " << cfg.iterations                        //
            << "\n# object-sizes: " << join(cfg.object_sizes)              //
            << "\n# worker-counts: " << join(cfg.worker_counts)            //
            << "\n# object-counts: " << join(cfg.object_counts)            //
            << "\n# repeated-read-counts-counts: "                         //
            << join(cfg.repeated_read_counts)                              //
            << "\n# experiments: "                                         //
            << join(vm["experiments"].as<std::vector<std::string>>())      //
            << "\n# Version: " << SSB_VERSION                              //
            << "\n# C++ SDK version: " << gc::version_string()             //
            << "\n# C++ SDK Compiler: " << gci::CompilerId()               //
            << "\n# C++ SDK Compiler Version: " << gci::CompilerVersion()  //
            << "\n# C++ SDK Compiler Flags: " << gci::compiler_flags()     //
            << "\n# gRPC version: " << grpc::Version()                     //
            << "\n# Protobuf version: " << SSB_PROTOBUF_VERSION            //
            << "\n# project-id: " << project                               //
            << "\n# tracing-rate: " << vm["tracing-rate"].as<double>()     //
            << std::endl;                                                  //

  run(cfg, make_experiments(vm));

  return EXIT_SUCCESS;
} catch (std::exception const& ex) {
  std::cerr << "Standard C++ exception caught " << ex.what() << "\n";
  return EXIT_FAILURE;
} catch (...) {
  std::cerr << "Unknown exception caught\n";
  return EXIT_FAILURE;
}

namespace {

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

class usage {
 public:
  usage()
      : mem_(mem_now()),
        clock_(std::chrono::steady_clock::now()),
        cpu_(cpu_now()) {}

  void record(config const& cfg, iteration_config const& iteration,
              std::int64_t bytes, auto span, auto attributes) const {
    auto const cpu_usage = cpu_now() - cpu_;
    auto const elapsed = std::chrono::steady_clock::now() - clock_;
    auto const mem_usage = mem_now() - mem_;

    auto per_byte = [bytes](auto value) {
      if (bytes == 0) return static_cast<double>(value);
      return static_cast<double>(value) / static_cast<double>(bytes);
    };
    auto throughput = [bytes](auto value) {
      if (std::abs(value) < std::numeric_limits<double>::epsilon()) {
        return static_cast<double>(0.0);
      }
      return static_cast<double>(bytes * 8) / static_cast<double>(value);
    };

    std::cout << "TP: "
              << throughput(
                     std::chrono::duration_cast<dseconds>(elapsed).count())
              << std::endl;

    cfg.latency->Record(
        std::chrono::duration_cast<dseconds>(elapsed).count(), attributes,
        opentelemetry::context::Context{}.SetValue("span", span));
    cfg.throughput->Record(
        throughput(std::chrono::duration_cast<dseconds>(elapsed).count()),
        attributes, opentelemetry::context::Context{}.SetValue("span", span));
    cfg.cpu->Record(per_byte(cpu_usage.count()), attributes,
                    opentelemetry::context::Context{}.SetValue("span", span));
    cfg.memory->Record(
        per_byte(mem_usage), attributes,
        opentelemetry::context::Context{}.SetValue("span", span));
    span->End();
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
  auto tracer =
      opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("ssb");

  auto generator = make_prng_bits_generator();
  auto data = std::make_shared<std::vector<char> const>(
      generate_random_data(generator));
  // Opentelemetry captures all string values as `std::string_view`. We need
  // to capture these strings in variables with lifetime longer than the loop.
  auto const sdk_version = gc::version_string();
  auto const grpc_version = grpc::Version();

  for (int i = 0; i != cfg.iterations; ++i) {
    auto const [experiment, runner] = pick_one(generator, experiments);

    auto const iteration = iteration_config{
        .object_size = pick_one(generator, cfg.object_sizes),
        .object_count = pick_one(generator, cfg.object_counts),
        .worker_count = pick_one(generator, cfg.worker_counts),
        .repeated_read_count = pick_one(generator, cfg.repeated_read_counts),
    };

    auto common_attributes =
        std::vector<std::pair<opentelemetry::nostd::string_view,
                              opentelemetry::common::AttributeValue>>{
            {"ssb.app", opentelemetry::nostd::string_view(kAppName.data(),
                                                          kAppName.size())},
            {"ssb.language", "cpp"},
            {"ssb.experiment", experiment},
            {"ssb.object-size", iteration.object_size},
            {"ssb.object-count", iteration.object_count},
            {"ssb.worker-count", iteration.worker_count},
            {"ssb.worker-count", iteration.repeated_read_count},
            {"ssb.deployment", cfg.deployment},
            {"ssb.instance", cfg.instance},
            {"ssb.region", cfg.region},
            {"ssb.version", SSB_VERSION},
            {"ssb.version.sdk", sdk_version},
            {"ssb.version.grpc", grpc_version},
            {"ssb.version.protobuf", SSB_PROTOBUF_VERSION},
            {"ssb.version.http-client", LIBCURL_VERSION},
        };

    auto with_op = [common_attributes](opentelemetry::nostd::string_view op) {
      auto attr = common_attributes;
      attr.emplace_back("ssb.op", op);
      return attr;
    };
    auto as_attributes = [](auto const& attr) {
      using value_type = typename std::decay_t<decltype(attr)>::value_type;
      using span_t = opentelemetry::nostd::span<value_type const>;
      return opentelemetry::common::MakeAttributes(
          span_t(attr.data(), attr.size()));
    };
    // Run the upload step in its own scope
    auto const objects = [&] {
      auto attributes = with_op("UPLOAD");
      auto span =
          tracer->StartSpan("ssb::maxt::upload",
                            opentelemetry::common::MakeAttributes(attributes));
      auto active = tracer->WithActiveSpan(span);
      auto const t = usage();
      auto objects = runner->upload(generator, cfg, data, iteration);
      t.record(cfg, iteration, objects.size() * iteration.object_size, span,
               as_attributes(attributes));
      return objects;
    }();

    {
      auto attributes = with_op("DOWNLOAD");
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
    }

    {
      auto span = tracer->StartSpan(
          "ssb::maxt::cleanup",
          opentelemetry::common::MakeAttributes(with_op("CLEANUP")));
      auto active = tracer->WithActiveSpan(span);
      runner->cleanup(cfg, iteration, objects);
      span->End();
    }
  }
}

auto upload_objects(gc::storage::Client client, int task_count, int task_id,
                    std::int64_t object_size, std::string const& bucket_name,
                    std::vector<std::string> const& object_names,
                    random_data data) {
  std::vector<object_metadata> objects;
  for (std::size_t i = 0; i != object_names.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& name = object_names[i];
    auto os = client.WriteObject(bucket_name, name);
    for (std::int64_t offset = 0; offset < object_size;) {
      auto n = std::min<std::int64_t>(data->size(), object_size - offset);
      os.write(data->data(), n);
      offset += n;
    }
    os.Close();
    auto meta = os.metadata();
    if (os.bad() || !meta) continue;
    objects.push_back({meta->bucket(), meta->name(), meta->generation()});
  }
  return objects;
}

auto download_objects(gc::storage::Client client, int task_count, int task_id,
                      std::vector<object_metadata> const& objects) {
  auto total_bytes = std::int64_t{0};
  std::vector<char> buffer(1 * kMiB);
  for (std::size_t i = 0; i != objects.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& meta = objects[i];
    auto is = client.ReadObject(meta.bucket_name, meta.object_name,
                                gc::storage::Generation(meta.generation));
    while (!is.bad() && !is.eof()) {
      is.read(buffer.data(), buffer.size());
      total_bytes += is.gcount();
    }
  }
  return total_bytes;
}

void delete_objects(gc::storage::Client client, int task_count, int task_id,
                    std::vector<object_metadata> const& objects) {
  std::vector<char> buffer(1 * kMiB);
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

  std::vector<object_metadata> upload(
      std::mt19937_64& generator, config const& cfg, random_data data,
      iteration_config const& iteration) override {
    auto object_names = generate_names(generator, iteration.object_count);

    std::vector<std::future<std::vector<object_metadata>>> tasks(
        iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, upload_objects, client_,
                        iteration.worker_count, i++, iteration.object_size,
                        std::cref(cfg.bucket_name), std::cref(object_names),
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
    std::vector<std::future<std::int64_t>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, download_objects, client_,
                        iteration.worker_count, i++, std::cref(objects));
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
    std::vector<std::future<void>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::async(std::launch::async, delete_objects, client_,
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
    gc::storage_experimental::AsyncClient client, int task_count, int task_id,
    std::int64_t object_size, std::string const& bucket_id,
    std::vector<std::string> const& object_names, random_data data) {
  gc::storage_experimental::BucketName bucket_name(bucket_id);
  std::vector<object_metadata> objects;
  for (std::size_t i = 0; i != object_names.size(); ++i) {
    if (i % task_count != task_id) continue;

    auto [writer, token] =
        (co_await client.StartUnbufferedUpload(bucket_name, object_names[i]))
            .value();
    auto offset = std::int64_t{0};
    while (offset < object_size) {
      auto n = std::min<std::size_t>(object_size - offset, data->size());
      token = (co_await writer.Write(std::move(token),
                                     gc::storage_experimental::WritePayload(
                                         std::string(data->data(), n))))
                  .value();
      offset += n;
    }
    auto m = (co_await writer.Finalize(std::move(token))).value();
    objects.push_back({m.bucket(), m.name(), m.generation()});
  }
  co_return objects;
}

gc::future<std::int64_t> async_download_objects(
    gc::storage_experimental::AsyncClient client, int task_count, int task_id,
    std::vector<object_metadata> const& objects) {
  auto total_bytes = std::int64_t{0};
  for (std::size_t i = 0; i != objects.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& object = objects[i];
    auto request = google::storage::v2::ReadObjectRequest{};
    request.set_bucket(object.bucket_name);
    request.set_object(object.object_name);
    request.set_generation(object.generation);
    auto [reader, token] =
        (co_await client.ReadObject(std::move(request))).value();
    while (token.valid()) {
      auto [payload, t] = (co_await reader.Read(std::move(token))).value();
      for (auto sv : payload.contents()) total_bytes += sv.size();
      token = std::move(t);
    }
  }
  co_return total_bytes;
}

gc::future<void> async_delete_objects(
    gc::storage_experimental::AsyncClient client, int task_count, int task_id,
    std::vector<object_metadata> const& objects) {
  for (std::size_t i = 0; i != objects.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& object = objects[i];
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

  std::vector<object_metadata> upload(
      std::mt19937_64& generator, config const& cfg, random_data data,
      iteration_config const& iteration) override {
    auto object_names = generate_names(generator, iteration.object_count);

    std::vector<gc::future<std::vector<object_metadata>>> tasks(
        iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return async_upload_objects(
          client_, iteration.worker_count, i++, iteration.object_size,
          std::cref(cfg.bucket_name), std::cref(object_names), data);
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
    std::vector<gc::future<std::int64_t>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return async_download_objects(client_, iteration.worker_count, i++,
                                    std::cref(objects));
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
    std::vector<gc::future<void>> tasks(iteration.worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return async_delete_objects(client_, iteration.worker_count, i++,
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

auto options() {
  return gc::Options{}
      .set<gc::storage::UploadBufferSizeOption>(256 * kKiB)
      .set<gc::OpenTelemetryTracingOption>(true);
}

auto make_json() { return gc::storage::Client(options()); }

auto make_grpc() {
  return gc::storage_experimental::DefaultGrpcClient(options());
}

auto make_dp() {
  return gc::storage_experimental::DefaultGrpcClient(
      options().set<gc::EndpointOption>(
          "google-c2p:///storage.googleapis.com"));
}

auto async_options(boost::program_options::variables_map const& vm,
                   std::string prefix, std::string_view endpoint) {
  auto opts = options().set<gc::EndpointOption>(std::string(endpoint));
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

auto constexpr kJson = "JSON"sv;
auto constexpr kGrpcCfe = "GRPC+CFE"sv;
auto constexpr kGrpcDp = "GRPC+DP"sv;
auto constexpr kAsyncGrpcCfe = "ASYNC+GRPC+CFE"sv;
auto constexpr kAsyncGrpcDp = "ASYNC+GRPC+DP"sv;

named_experiments make_experiments(
    boost::program_options::variables_map const& vm) {
  named_experiments ne;
  for (auto const& name : vm["experiments"].as<std::vector<std::string>>()) {
    if (name == kJson) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_json()));
    } else if (name == kGrpcCfe) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_grpc()));
    } else if (name == kGrpcDp) {
      ne.emplace(name, std::make_shared<sync_experiment>(make_dp()));
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
  // We expect the library to use less memory than the transferred size, that is
  // why we stream the data. Use exponentially growing bucket sizes, since we
  // have no better ideas.
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

boost::program_options::variables_map parse_args(int argc, char* argv[]) {
  auto const cores = static_cast<int>(std::thread::hardware_concurrency());

  namespace po = boost::program_options;
  po::options_description desc(
      "A simple publisher application with Open Telemetery enabled");
  // The following empty line comments are for readability.
  desc.add_options()                      //
      ("help,h", "produce help message")  //
      // Benchmark options
      ("bucket", po::value<std::string>()->required(),
       "the name of a Google Cloud Storage bucket. The benchmark uses this"
       " bucket to upload and download objects and measures the latency.")  //
      ("deployment", po::value<std::string>()->default_value("development"),
       "a short string describing where the benchmark is deployed, e.g."
       " development, or GKE, or GCE.")  //
      ("iterations", po::value<int>()->default_value(kDefaultIterations),
       "the number of iterations before exiting the test")  //
      // Create enough workers to keep the available CPUs busy rea
      ("worker-counts",
       po::value<std::vector<int>>()->multitoken()->default_value(
           {cores, 2 * cores}, std::format("[ {}, {} ]", cores, 2 * cores)),
       "the object sizes used in the benchmark.")
      // Create enough objects, such that every worker has at least one.
      ("object-counts",
       po::value<std::vector<int>>()->multitoken()->default_value(
           {2 * cores}, std::format("[ {} ]", 2 * cores)),
       "the object counts used in the benchmark.")
      // Make the objects large enough to mask the TTFB delays.
      ("object-sizes",
       po::value<std::vector<std::int64_t>>()->multitoken()->default_value(
           {256 * kMiB}, "[ 256MiB ]"),
       "the object sizes used in the benchmark.")
      // By default read the data only once. That is a fairly cold dataset,
      // increase this number to simulate hotter datasets.
      ("repeated-read-counts",
       po::value<std::vector<int>>()->multitoken()->default_value({1}, "[ 1 ]"),
       "read each object multiple times to simulate 'hot' data.")  //
      ("experiments",
       po::value<std::vector<std::string>>()->multitoken()->default_value(
           {std::string(kJson), std::string(kGrpcCfe),
            std::string(kAsyncGrpcCfe)},
           std::format("[ {}, {}, {} ]", kJson, kGrpcCfe, kAsyncGrpcCfe)),
       "the experiments used in the benchmark.")
      // gRPC configuration options
      ("cfe-thread-pool", po::value<int>(), "CFE background thread pool.")  //
      ("cfe-channels", po::value<int>(), "the number of CFE channels.")     //
      ("dp-thread-pool", po::value<int>(), "DP background thread pool.")    //
      ("dp-channels", po::value<int>(), "the number of DP channels.")       //
      // Open Telemetry Processor options
      ("project-id", po::value<std::string>()->required(),
       "a Google Cloud Project id. The benchmark sends its results to this"
       " project as Cloud Monitoring metrics and Cloud Trace traces.")  //
      ("tracing-rate", po::value<double>()->default_value(kDefaultSampleRate),
       "otel::BasicTracingRateOption value")  //
      ("max-queue-size", po::value<int>()->default_value(2048),
       "set the max queue size for open telemetery")  //
      ;

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  if (vm.count("help") || argc == 1) {
    std::cerr << "Usage: " << argv[0] << "\n";
    std::cerr << desc << "\n";
    std::exit(argc == 1 ? EXIT_FAILURE : EXIT_SUCCESS);
  }
  po::notify(vm);
  return vm;
}

}  // namespace

void* operator new(std::size_t count) {
  allocated_bytes.fetch_add(count);
  return std::malloc(count);
}
