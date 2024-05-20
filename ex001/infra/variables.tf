# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

variable "project" {
  default = "coryan-test"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-f"
}

variable "replicas" {
  default = 1
}

variable "experiment" {
  default = "ex001"
}

# use `-var=app_version_cpp=v2` to redeploy when the changes do not trigger
# a terraform change. See mig/cpp/main.tf for details.
variable "app_version_cpp" {
  default = "v1"
}
