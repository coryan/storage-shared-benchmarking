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

#include <google/cloud/internal/build_info.h>
#include <google/cloud/internal/compiler_info.h>
#include <google/cloud/opentelemetry/configure_basic_tracing.h>
#include <google/cloud/opentelemetry/monitoring_exporter.h>
#include <google/cloud/opentelemetry/resource_detector.h>
#include <google/cloud/opentelemetry_options.h>
#include <google/cloud/project.h>
#include <google/cloud/storage/async/client.h>
#include <google/cloud/storage/client.h>
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

std::vector<int> get_worker_counts(
    boost::program_options::variables_map const& vm);
std::vector<int> get_object_counts(
    boost::program_options::variables_map const& vm);
std::vector<std::int64_t> get_object_sizes(
    boost::program_options::variables_map const& vm);

using histogram_ptr =
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>;

struct config {
  std::vector<int> worker_counts;
  std::vector<int> object_counts;
  std::vector<std::int64_t> object_sizes;
  std::string bucket_name;
  std::string deployment;
  std::string instance;
  std::string region;
  int iterations;
  std::chrono::seconds iteration_time;
  histogram_ptr latency;
  histogram_ptr cpu;
  histogram_ptr memory;
};

using random_data = std::shared_ptr<std::vector<char> const>;

struct experiment {
  virtual void init(std::string bucket_name, std::int64_t object_size,
                    std::vector<std::string> object_names,
                    random_data data) = 0;
  virtual void run(std::string bucket_name, int worker_count,
                   std::int64_t object_size,
                   std::vector<std::string> object_names, random_data data) = 0;
  virtual void cleanup(std::string bucket_name,
                       std::vector<std::string> object_names) = 0;
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
  // Using the `internal` namespace is frowned upon. The C++ SDK team may change
  // the types and functions in this namespace at any time. If this ever breaks
  // we will find out at compile time, and will need to detect the compiler and
  // build flags ourselves.
  namespace gci = ::google::cloud::internal;
  std::cout << "## Starting continuous GCS C++ SDK benchmark"              //
            << "\n# project-id: " << project                               //
            << "\n# bucket: " << bucket_name                               //
            << "\n# deployment: " << deployment                            //
            << "\n# worker-counts: " << join(get_worker_counts(vm))        //
            << "\n# object-counts: " << join(get_object_counts(vm))        //
            << "\n# object-sizes: " << join(get_object_sizes(vm))          //
            << "\n# instance: " << instance                                //
            << "\n# tracing-rate: " << tracing_rate                        //
            << "\n# Version: " << SSB_VERSION                              //
            << "\n# C++ SDK version: " << gc::version_string()             //
            << "\n# C++ SDK Compiler: " << gci::CompilerId()               //
            << "\n# C++ SDK Compiler Version: " << gci::CompilerVersion()  //
            << "\n# C++ SDK Compiler Flags: " << gci::compiler_flags()     //
            << "\n# gRPC version: " << grpc::Version()                     //
            << "\n# Protobuf version: " << SSB_PROTOBUF_VERSION            //
            << std::endl;                                                  //

  auto const tracing = gc::otel::ConfigureBasicTracing(
      project,
      gc::Options{}.set<gc::otel::BasicTracingRateOption>(tracing_rate));

  auto provider =
      make_meter_provider(google::cloud::Project(project), instance);

  // Create a histogram to capture the performance results.
  auto meter = provider->GetMeter(std::string{kAppName}, kVersion, kSchema);
  histogram_ptr latency = meter->CreateDoubleHistogram(
      kLatencyHistogramName, kLatencyDescription, kLatencyHistogramUnit);
  histogram_ptr cpu = meter->CreateDoubleHistogram(
      kCpuHistogramName, kCpuDescription, kCpuHistogramUnit);
  histogram_ptr memory = meter->CreateDoubleHistogram(
      kMemoryHistogramName, kMemoryDescription, kMemoryHistogramUnit);

  auto cfg = config{
      .worker_counts = get_worker_counts(vm),
      .object_counts = get_object_counts(vm),
      .object_sizes = get_object_sizes(vm),
      .bucket_name = std::move(bucket_name),
      .deployment = deployment,
      .instance = instance,
      .region = discover_region(),
      .iterations = vm["iterations"].as<int>(),
      .iteration_time = std::chrono::seconds(vm["iteration-seconds"].as<int>()),
      .latency = std::move(latency),
      .cpu = std::move(cpu),
      .memory = std::move(memory),
  };

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

std::vector<int> get_worker_counts(
    boost::program_options::variables_map const& vm) {
  auto const l = vm.find("object-counts");
  if (l != vm.end()) return l->second.as<std::vector<int>>();
  return {16, 32, 64};
}

std::vector<int> get_object_counts(
    boost::program_options::variables_map const& vm) {
  auto const l = vm.find("object-counts");
  if (l != vm.end()) return l->second.as<std::vector<int>>();
  return {16, 32, 64};
}

std::vector<std::int64_t> get_object_sizes(
    boost::program_options::variables_map const& vm) {
  auto const l = vm.find("object-sizes");
  if (l != vm.end()) return l->second.as<std::vector<std::int64_t>>();
  return {1024 * kMiB};
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

class usage {
 public:
  usage()
      : mem_(mem_now()),
        clock_(std::chrono::steady_clock::now()),
        cpu_(cpu_now()) {}

  void record(config const& cfg, std::uint64_t object_size, auto span,
              auto attributes) const {
    auto const cpu_usage = cpu_now() - cpu_;
    auto const elapsed = std::chrono::steady_clock::now() - clock_;
    auto const mem_usage = mem_now() - mem_;

    auto scale = [object_size](auto value) {
      if (object_size == 0) return static_cast<double>(value);
      return static_cast<double>(value) / static_cast<double>(object_size);
    };

    cfg.latency->Record(
        std::chrono::duration_cast<dseconds>(elapsed).count(), attributes,
        opentelemetry::context::Context{}.SetValue("span", span));
    cfg.cpu->Record(scale(cpu_usage.count()), attributes,
                    opentelemetry::context::Context{}.SetValue("span", span));
    cfg.memory->Record(
        scale(mem_usage), attributes,
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
    auto const object_size = pick_one(generator, cfg.object_sizes);
    auto const object_count = pick_one(generator, cfg.object_counts);
    auto const worker_count = pick_one(generator, cfg.worker_counts);
    auto const [experiment, runner] = pick_one(generator, experiments);
    auto const object_names = generate_names(generator, object_count);
    runner->init(cfg.bucket_name, object_size, object_names, data);

    auto const iteration_end =
        std::chrono::steady_clock::now() + cfg.iteration_time;
    while (std::chrono::steady_clock::now() < iteration_end) {
      auto common_attributes =
          std::vector<std::pair<opentelemetry::nostd::string_view,
                                opentelemetry::common::AttributeValue>>{
              {"ssb.app", opentelemetry::nostd::string_view(kAppName.data(),
                                                            kAppName.size())},
              {"ssb.language", "cpp"},
              {"ssb.object-size", object_size},
              {"ssb.object-count", object_count},
              {"ssb.worker-count", worker_count},
              {"ssb.experiment", experiment},
              {"ssb.deployment", cfg.deployment},
              {"ssb.instance", cfg.instance},
              {"ssb.region", cfg.region},
              {"ssb.version", SSB_VERSION},
              {"ssb.version.sdk", sdk_version},
              {"ssb.version.grpc", grpc_version},
              {"ssb.version.protobuf", SSB_PROTOBUF_VERSION},
              {"ssb.version.http-client", LIBCURL_VERSION},
          };

      auto as_attributes = [](auto const& attr) {
        using value_type = typename std::decay_t<decltype(attr)>::value_type;
        using span_t = opentelemetry::nostd::span<value_type const>;
        return opentelemetry::common::MakeAttributes(
            span_t(attr.data(), attr.size()));
      };

      auto iteration_span = tracer->StartSpan(
          "ssb::maxt::iteration",
          opentelemetry::common::MakeAttributes(common_attributes));
      auto iteration = tracer->WithActiveSpan(iteration_span);
      auto const t = usage();
      runner->run(cfg.bucket_name, worker_count, object_size, object_names,
                  data);
      t.record(cfg, object_size, iteration_span,
               as_attributes(common_attributes));
    }

    runner->cleanup(cfg.bucket_name, object_names);
  }
}

void upload_objects(gc::storage::Client client, int task_count, int task_id,
                    std::int64_t object_size, std::string const& bucket_name,
                    std::vector<std::string> const& object_names,
                    random_data data) {
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
  }
}

void download_objects(gc::storage::Client client, int task_count, int task_id,
                      std::string const& bucket_name,
                      std::vector<std::string> const& object_names) {
  std::vector<char> buffer(1 * kMiB);
  for (std::size_t i = 0; i != object_names.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& name = object_names[i];
    auto is = client.ReadObject(bucket_name, name);
    while (!is.bad() && !is.eof()) {
      is.read(buffer.data(), buffer.size());
    }
  }
}

void delete_objects(gc::storage::Client client, int task_count, int task_id,
                    std::string const& bucket_name,
                    std::vector<std::string> const& object_names) {
  std::vector<char> buffer(1 * kMiB);
  for (std::size_t i = 0; i != object_names.size(); ++i) {
    if (i % task_count != task_id) continue;
    auto const& name = object_names[i];
    (void)client.DeleteObject(bucket_name, name);
  }
}

class sync_download : public experiment {
 public:
  explicit sync_download(gc::storage::Client c) : client_(std::move(c)) {}

  void init(std::string bucket_name, std::int64_t object_size,
            std::vector<std::string> object_names, random_data data) override {
    auto const task_count = 2 * std::thread::hardware_concurrency();
    std::vector<std::jthread> tasks(task_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::jthread(upload_objects, client_, task_count, i++, object_size,
                          std::cref(bucket_name), std::cref(object_names),
                          data);
    });
  }

  void run(std::string bucket_name, int worker_count, std::int64_t object_size,
           std::vector<std::string> object_names,
           random_data /*data*/) override {
    std::vector<std::jthread> tasks(worker_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::jthread(download_objects, client_, worker_count, i++,
                          std::cref(bucket_name), std::cref(object_names));
    });
  }

  void cleanup(std::string bucket_name,
               std::vector<std::string> object_names) override {
    auto const task_count = 2 * std::thread::hardware_concurrency();
    std::vector<std::jthread> tasks(task_count);
    std::generate(tasks.begin(), tasks.end(), [&, i = 0]() mutable {
      return std::jthread(delete_objects, client_, task_count, i++,
                          std::cref(bucket_name), std::cref(object_names));
    });
  }

 private:
  gc::storage::Client client_;
};

named_experiments make_experiments(
    boost::program_options::variables_map const& vm) {
  return named_experiments{
      {"JSON+DOWNLOADS", std::make_shared<sync_download>(gc::storage::Client())},
  };
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
  auto increment = 2ms;
  // For the first 100ms use 2ms buckets. We need higher resolution in this
  // area for 100KB uploads and downloads.
  for (int i = 0; i != 50; ++i) {
    boundaries.push_back(
        std::chrono::duration_cast<dseconds>(boundary).count());
    boundary += increment;
  }
  // The remaining buckets are 10ms wide, and then 20ms, and so forth. We stop
  // at 300,000ms (5 minutes) because any latency over that is too high for this
  // benchmark.
  boundary = 100ms;
  increment = 10ms;
  for (int i = 0; i != 150 && boundary <= 300s; ++i) {
    boundaries.push_back(
        std::chrono::duration_cast<dseconds>(boundary).count());
    if (i != 0 && i % 10 == 0) increment *= 2;
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
  add_histogram_view(p, kCpuHistogramName, kCpuDescription, kCpuHistogramUnit,
                     make_cpu_histogram_boundaries());
  add_histogram_view(p, kMemoryHistogramName, kMemoryDescription,
                     kMemoryHistogramUnit, make_memory_histogram_boundaries());

  return provider;
}

boost::program_options::variables_map parse_args(int argc, char* argv[]) {
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
      ("iteration-seconds", po::value<int>()->default_value(300),
       "the duration of each iteration")  //
      ("worker-counts", po::value<std::vector<int>>()->multitoken(),
       "the object sizes used in the benchmark.")  //
      ("object-sizes", po::value<std::vector<std::int64_t>>()->multitoken(),
       "the object sizes used in the benchmark.")  //
      ("experiments", po::value<std::vector<std::string>>()->multitoken(),
       "the experiments used in the benchmark.")  //
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
