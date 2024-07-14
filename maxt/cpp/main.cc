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
#include <google/cloud/project.h>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <iterator>
#include <new>
#include <numeric>
#include <string>
#include <thread>

import maxt;

void* operator new(std::size_t count) {
  maxt::count_allocations(count);
  return std::malloc(count);
}

int main(int argc, char* argv[]) try {
  auto const vm = maxt::parse_args(argc, argv);
  auto const cfg = maxt::make_config(vm);

  auto join = [](auto const& collection) {
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

  std::cout << "## Starting continuous GCS C++ SDK benchmark"                //
            << "\n# bucket: " << cfg.bucket_name                             //
            << "\n# deployment: " << cfg.deployment                          //
            << "\n# instance: " << cfg.instance                              //
            << "\n# region: " << cfg.region                                  //
            << "\n# iterations: " << cfg.iterations                          //
            << "\n# object-sizes: " << join(cfg.object_sizes)                //
            << "\n# read-worker-counts: " << join(cfg.read_worker_counts)    //
            << "\n# write-worker-counts: " << join(cfg.write_worker_counts)  //
            << "\n# object-counts: " << join(cfg.object_counts)              //
            << "\n# repeated-read-counts-counts: "                           //
            << join(cfg.repeated_read_counts)                                //
            << "\n# experiments: "                                           //
            << join(vm["experiments"].as<std::vector<std::string>>())        //
            << "\n# Version: " << cfg.ssb_version                            //
            << "\n# C++ SDK version: " << cfg.sdk_version                    //
            << "\n# gRPC version: " << cfg.grpc_version                      //
            << "\n# Protobuf version: " << cfg.protobuf_version              //
            << "\n# C++ SDK Compiler: " << gci::CompilerId()                 //
            << "\n# C++ SDK Compiler Version: " << gci::CompilerVersion()    //
            << "\n# C++ SDK Compiler Flags: " << gci::compiler_flags()       //
            << "\n# project-id: " << vm["project-id"].as<std::string>()      //
            << "\n# tracing-rate: " << vm["tracing-rate"].as<double>()       //
            << std::endl;                                                    //

  auto const mts = maxt::make_metrics(vm, cfg);
  std::vector<std::jthread> runners;
  std::generate_n(
      std::back_inserter(runners), vm["runners"].as<int>(), [&mts, &cfg, &vm] {
        return std::jthread(maxt::run, cfg, mts, maxt::make_experiments(vm));
      });

  return EXIT_SUCCESS;
} catch (std::exception const& ex) {
  std::cerr << "Standard C++ exception caught " << ex.what() << "\n";
  return EXIT_FAILURE;
} catch (...) {
  std::cerr << "Unknown exception caught\n";
  return EXIT_FAILURE;
}
