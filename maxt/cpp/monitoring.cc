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
#include <google/cloud/opentelemetry/monitoring_exporter.h>
#include <google/cloud/opentelemetry/resource_detector.h>
#include <google/cloud/opentelemetry/trace_exporter.h>
#include <boost/container_hash/hash.hpp>
#include <boost/program_options/variables_map.hpp>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/sync_instruments.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/metrics/aggregation/aggregation_config.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/samplers/trace_id_ratio_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

export module maxt:monitoring;
import :config;
import :constants;
import :dseconds;

namespace maxt_internal {

struct measurement_key {
  std::string operation;
  iteration_config iteration;

  auto operator<=>(measurement_key const&) const = default;
};

struct measurement_value {
  double throughput;
  double cpu;
  double memory;
};

using histogram_ptr =
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>;

}  // namespace maxt_internal

namespace std {
template <>
struct std::hash<maxt_internal::measurement_key> {
  std::size_t operator()(
      maxt_internal::measurement_key const& key) const noexcept {
    std::size_t seed = 0;
    boost::hash_combine(seed, key.operation);
    boost::hash_combine(seed, key.iteration.experiment);
    boost::hash_combine(seed, key.iteration.object_size);
    boost::hash_combine(seed, key.iteration.object_count);
    boost::hash_combine(seed, key.iteration.worker_count);
    boost::hash_combine(seed, key.iteration.repeated_read_count);
    return seed;
  }
};

}  // namespace std

namespace maxt_internal {

using namespace std::literals;

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

class measurements {
 public:
  void store(iteration_config const& iteration, std::string_view op,
             measurement_value value) {
    std::lock_guard lk(mu_);
    values_.emplace(
        measurement_key{.operation = std::string(op), .iteration = iteration},
        std::move(value));
  }

  void fetch() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<measurement_key, measurement_value> values_;
};

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
  auto exporter = google::cloud::otel_internal::MakeMonitoringExporter(
      project, google::cloud::monitoring_v3::MakeMetricServiceConnection());

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

struct metrics {
  std::shared_ptr<measurements> measurements;
  histogram_ptr latency;
  histogram_ptr throughput;
  histogram_ptr cpu;
  histogram_ptr memory;
};

auto make_metrics(boost::program_options::variables_map const& vm,
                  config const& cfg) {
  auto tracer_provider = maxt_internal::make_tracer_provider(vm);
  auto meter_provider = maxt_internal::make_meter_provider(
      google::cloud::Project(vm["project-id"].as<std::string>()), cfg.instance);
  auto meter =
      meter_provider->GetMeter(std::string{kAppName}, kVersion, kSchema);

  auto latency = meter->CreateDoubleHistogram(
      kLatencyHistogramName, kLatencyDescription, kLatencyHistogramUnit);
  auto throughput = meter->CreateDoubleHistogram(kThroughputHistogramName,
                                                 kThroughputDescription,
                                                 kThroughputHistogramUnit);
  auto cpu = meter->CreateDoubleHistogram(kCpuHistogramName, kCpuDescription,
                                          kCpuHistogramUnit);
  auto memory = meter->CreateDoubleHistogram(
      kMemoryHistogramName, kMemoryDescription, kMemoryHistogramUnit);

  auto shared = [](auto u) { return histogram_ptr(std::move(u)); };

  auto m = std::make_shared<measurements>();
  // TODO(coryan) - use `m` to setup callbacks for GAUGE metrics
  return metrics{
      .measurements = std::move(m),                 //
      .latency = shared(std::move(latency)),        //
      .throughput = shared(std::move(throughput)),  //
      .cpu = shared(std::move(cpu)),                //
      .memory = shared(std::move(memory)),          //
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

}  // namespace maxt_internal
