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

variable "project" {}
variable "region" {}

locals {
  builds = {
    w1r3-go = {
      config = "w1r3/go/cloudbuild.yaml"
    }
  }
  repository = "projects/storage-sdk-prober-project/locations/us-central1/connections/github/repositories/googleapis-storage-shared-benchmarking"
}

resource "google_cloudbuild_trigger" "pull-request" {
  for_each = tomap(local.builds)
  location = var.region
  name     = "${each.key}-pr"
  filename = each.value.config
  tags     = ["pr", "pull-request", "name:${each.key}"]

  repository_event_config {
#    repository = "projects/${var.project}/locations/${var.region}/connections/googleapis-storage-shared-benchmarking"
    repository = local.repository
    pull_request {
      branch          = "^main$"
      comment_control = "COMMENTS_ENABLED_FOR_EXTERNAL_CONTRIBUTORS_ONLY"
    }
  }
  substitutions = {
    _TAG = "pr"
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}

resource "google_cloudbuild_trigger" "post-merge" {
  for_each = tomap(local.builds)
  location = var.region
  name     = "${each.key}-pm"
  filename = each.value.config
  tags     = ["pm", "post-merge", "push", "name:${each.key}"]

  repository_event_config {
#    repository = "projects/${var.project}/locations/${var.region}/connections/googleapis-storage-shared-benchmarking"
    repository = local.repository
    push {
      branch = "^main$"
    }
  }
  substitutions = {
    _TAG = "pr"
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}
