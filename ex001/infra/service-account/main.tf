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
  type = string
}

variable "experiment" {
  type = string
}

variable "buckets" {
  type = set(string)
}

# Create a service account to run the benchmarks. This service account will
# be granted permissions to:
# - Read and write to the bucket created above
# - Publish new Cloud Trace traces
# - Publish new metrics to Cloud Monitoring
resource "google_service_account" "default" {
  account_id   = "${var.experiment}-runner"
  display_name = "Experiment ${var.experiment} Runner"
  description  = "The principal used by experiment ${var.experiment}"
}

output "email" {
  value = google_service_account.default.email
}

# Grant the service account permissions on the bucket
resource "google_storage_bucket_iam_binding" "grant-sa-permissions-on-bucket" {
  for_each = var.buckets
  bucket   = each.value
  role     = "roles/storage.admin"
  members = [
    "serviceAccount:${google_service_account.default.email}",
  ]
  depends_on = [google_service_account.default]
}

# Grant the service account permissions to publish metrics, profiles, traces and
# logs on the project
resource "google_project_iam_member" "grant-sa-cloud-trace-permissions" {
  project    = var.project
  role       = "roles/cloudtrace.agent"
  member     = "serviceAccount:${google_service_account.default.email}"
  depends_on = [google_service_account.default]
}

resource "google_project_iam_member" "grant-sa-cloud-monitoring-permissions" {
  project    = var.project
  role       = "roles/monitoring.metricWriter"
  member     = "serviceAccount:${google_service_account.default.email}"
  depends_on = [google_service_account.default]
}

resource "google_project_iam_member" "grant-sa-cloud-profiler-permissions" {
  project    = var.project
  role       = "roles/cloudprofiler.agent"
  member     = "serviceAccount:${google_service_account.default.email}"
  depends_on = [google_service_account.default]
}

resource "google_project_iam_member" "grant-sa-cloud-logging-permissions" {
  project    = var.project
  role       = "roles/logging.logWriter"
  member     = "serviceAccount:${google_service_account.default.email}"
  depends_on = [google_service_account.default]
}

resource "google_storage_bucket_iam_binding" "grant-sa-gcr-pull-permissions" {
  bucket = "artifacts.${var.project}.appspot.com"
  role   = "roles/storage.objectViewer"
  members = [
    "serviceAccount:${google_service_account.default.email}",
  ]
  depends_on = [google_service_account.default]
}
