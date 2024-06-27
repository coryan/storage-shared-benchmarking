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
#include <boost/program_options.hpp>
#include <format>
#include <iostream>
#include <string_view>
#include <thread>

export module maxt;

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

auto constexpr kDefaultIterations = 1'000'000;
auto constexpr kDefaultSampleRate = 0.05;

export boost::program_options::variables_map parse_args(int argc,
                                                        char* argv[]) {
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
      //
      ("runners", po::value<int>()->default_value(1),
       "the number of runners to run in parallel.")
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
      ("max-queue-size", po::value<std::size_t>()->default_value(1'000'000),
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
