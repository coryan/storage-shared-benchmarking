# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM fedora:40 AS build

# Install the minimal development tools:
RUN dnf makecache && \
    dnf install -y cmake curl findutils gcc-c++ git make ninja-build \
        patch unzip tar wget zip

# We compile Abseil, Protobuf and gRPC from source to pick the latest versions
# and to enable OTel support in gRPC. But for other libraries the Fedora
# packages are recent enough.
RUN dnf makecache && \
    dnf install -y boost-devel c-ares-devel google-crc32c-devel json-devel \
        libcurl-devel openssl-devel re2-devel zlib-devel

WORKDIR /var/tmp/build
RUN curl -fsSL https://github.com/abseil/abseil-cpp/archive/20240116.2.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
        -GNinja -S . -B .build \
        -DCMAKE_CXX_STANDARD=20 \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DABSL_BUILD_TESTING=OFF \
        -DABSL_PROPAGATE_CXX_STD=ON && \
    cmake --build .build && cmake --install .build && \
    rm -fr .build
        
WORKDIR /var/tmp/build/opentelemetry-cpp
RUN curl -fsSL https://github.com/open-telemetry/opentelemetry-cpp/archive/v1.16.0.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
        -G Ninja -S . -B .build \
        -DCMAKE_CXX_STANDARD=20 \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_EXAMPLES=OFF \
        -DWITH_ABSEIL=ON \
        -DWITH_DEPRECATED_SDK_FACTORY=OFF \
        -DBUILD_TESTING=OFF \
        -DOPENTELEMETRY_INSTALL=ON \
        -DOPENTELEMETRY_ABI_VERSION_NO=2 && \
    cmake --build .build && cmake --install .build && \
    rm -fr .build

WORKDIR /var/tmp/build/protobuf
RUN curl -fsSL https://github.com/protocolbuffers/protobuf/archive/v27.2.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
        -GNinja -S . -B .build \
        -DCMAKE_CXX_STANDARD=20 \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -Dprotobuf_BUILD_TESTS=OFF \
        -Dprotobuf_ABSL_PROVIDER=package && \
    cmake --build .build && cmake --install .build && \
    rm -fr .build
    
WORKDIR /var/tmp/build/grpc
RUN curl -fsSL https://github.com/grpc/grpc/archive/v1.64.2.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
        -GNinja -S . -B .build \
        -DCMAKE_CXX_STANDARD=20 \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DgRPC_ABSL_PROVIDER=package \
        -DgRPC_CARES_PROVIDER=package \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_RE2_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package \
        -DgRPC_ZLIB_PROVIDER=package \
        -DgRPC_OPENTELEMETRY_PROVIDER=package \
        -DgRPC_BUILD_GRPCPP_OTEL_PLUGIN=ON && \
    cmake --build .build && cmake --install .build && \
    rm -fr .build

WORKDIR /var/tmp/build/google-cloud-cpp
RUN curl -fsSL https://github.com/googleapis/google-cloud-cpp/archive/e4ed3a463b4b9b30dc931c1082ee4689d298c621.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
      -G Ninja -S . -B .build \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=OFF \
      -DBUILD_TESTING=OFF \
      -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
      -DGOOGLE_CLOUD_CPP_ENABLE=storage,experimental-storage_grpc,monitoring,opentelemetry && \
    cmake --build .build && cmake --install .build && \
    rm -fr .build

# Update the ld.conf cache in case any libraries were installed in /usr/local/lib*
RUN ldconfig /usr/local/lib*

RUN dnf makecache && \
    dnf install -y clang clang-tools-extra

WORKDIR /var/tmp/build/maxt
COPY .git/ /var/tmp/build/maxt/.git
COPY maxt/ /var/tmp/build/maxt/maxt
RUN CXX=clang++ CC=clang cmake -G Ninja -S maxt/cpp -B .build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build .build

FROM registry.fedoraproject.org/fedora-minimal:40 AS deploy

RUN microdnf makecache && \
    microdnf install -y boost c-ares libcurl google-crc32c re2 zlib && \
    microdnf clean all

COPY --from=build /var/tmp/build/maxt/.build/maxt /r/maxt
