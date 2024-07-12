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
#include <google/cloud/opentelemetry_options.h>
#include <google/cloud/project.h>
#include <google/cloud/storage/async/client.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/grpc_plugin.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/version.h>
#include <boost/program_options/variables_map.hpp>
#include <curl/curlver.h>
#include <grpcpp/grpcpp.h>
#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
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

export module maxt;
import :constants;
import :config;
import :experiment;
import :async_experiment;
import :sync_experiment;
import :parse_args;
import :resource_usage;

namespace {

namespace gc = ::google::cloud;

auto constexpr kRandomDataSize = 32 * kMiB;

}  // namespace

namespace maxt_internal {

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

template <typename Collection>
auto pick_one(std::mt19937_64& generator, Collection const& collection) {
  auto index = std::uniform_int_distribution<std::size_t>(
      0, std::size(collection) - 1)(generator);
  return *std::next(std::begin(collection), index);
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

void run(config cfg, metrics mts, named_experiments experiments) {
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

    // Run the upload step in its own scope
    auto const objects = [&] {
      auto attributes = make_common_attributes(cfg, iteration, "UPLOAD");
      auto span =
          tracer->StartSpan("ssb::maxt::upload",
                            opentelemetry::common::MakeAttributes(attributes));
      auto active = tracer->WithActiveSpan(span);
      auto const t = usage();
      auto objects = runner->upload(generator, mts, cfg, iteration, data);
      auto const bytes = objects.size() * iteration.object_size;
      t.record(mts, cfg, iteration, bytes, "UPLOAD");
      span->End();
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
      auto const bytes = runner->download(mts, cfg, iteration, repeated);
      t.record(mts, cfg, iteration, bytes, "DOWNLOAD");
      download_span->End();
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

auto options(boost::program_options::variables_map const& vm) {
  using namespace std::chrono_literals;
  auto enable_tracing = vm["tracing-rate"].as<double>() != 0.0;
  return gc::Options{}
      .set<gc::storage::TransferStallMinimumRateOption>(
          vm["upload-stall-rate-MiB"].as<int>() * kMiB)
      .set<gc::storage::TransferStallTimeoutOption>(1s)
      .set<gc::storage::DownloadStallMinimumRateOption>(
          vm["download-stall-rate-MiB"].as<int>() * kMiB)
      .set<gc::storage::DownloadStallTimeoutOption>(1s)
      .set<gc::storage::UploadBufferSizeOption>(256 * kKiB)
      .set<gc::storage_experimental::HttpVersionOption>(
          vm["http-version"].as<std::string>())
      .set<gc::OpenTelemetryTracingOption>(enable_tracing);
}

auto make_json(boost::program_options::variables_map const& vm) {
  return gc::storage::Client(options(vm));
}

auto grpc_options(boost::program_options::variables_map const& vm,
                  std::string prefix, std::string_view endpoint) {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_MAX_CONCURRENT_STREAMS,
              vm["max-concurrent-streams"].as<int>());
  auto opts = options(vm)
                  .set<gc::EndpointOption>(std::string(endpoint))
                  .set<gc::GrpcChannelArgumentsNativeOption>(std::move(args));
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

}  // namespace maxt_internal

export namespace maxt {

auto parse_args(int argc, char* argv[]) { return ::parse_args(argc, argv); }

void count_allocations(std::size_t count) {
  maxt_internal::allocated_bytes.fetch_add(count);
}

auto make_prng_bits_generator() {
  return maxt_internal::make_prng_bits_generator();
}

auto make_config(boost::program_options::variables_map const& vm) {
  auto generator = maxt::make_prng_bits_generator();

  return maxt_internal::config{
      .bucket_name = vm["bucket"].as<std::string>(),
      .deployment = vm["deployment"].as<std::string>(),
      .instance = maxt_internal::generate_uuid(generator),
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
  };
}

auto make_metrics(boost::program_options::variables_map const& vm,
                  maxt_internal::config const& cfg) {
  return maxt_internal::make_metrics(vm, cfg);
}

maxt_internal::named_experiments make_experiments(
    boost::program_options::variables_map const& vm) {
  return maxt_internal::make_experiments(vm);
}

void run(maxt_internal::config cfg, maxt_internal::metrics mts,
         maxt_internal::named_experiments experiments) {
  return maxt_internal::run(std::move(cfg), std::move(mts),
                            std::move(experiments));
}

}  // namespace maxt
