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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.29.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

# Create a bucket to store the data used in the benchmark. This data is expected
# to be ephemeral. Any data older than 15 days gets automatically removed.
# Only very long running benchmarks may need data for longer than this, and they
# can refresh the data periodically to avoid running afoul of the lifecycle
# rule.
resource "google_storage_bucket" "o0" {
  name                        = "gcs-grpc-team-ex001-o0-${var.region}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "o16" {
  name                        = "gcs-grpc-team-ex001-o16-${var.region}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "o128" {
  name                        = "gcs-grpc-team-ex001-o128-${var.region}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "Delete"
    }
  }
}

module "sa" {
  source     = "./service-account"
  project    = var.project
  experiment = var.experiment
  buckets = [
    google_storage_bucket.o0.name,
    google_storage_bucket.o16.name,
    google_storage_bucket.o128.name
  ]
}
# 
# module "ex001-o128-cpp" {
# source          = "./mig/cpp"
# project         = var.project
# bucket          = google_storage_bucket.o128.name
# object_size     = 128 * 1024 * 1024
# region          = var.region
# replicas        = var.replicas
# service_account = module.mig-sa.email
# app_version     = var.app_version_cpp
# depends_on      = [module.mig-sa]
# }
# 
