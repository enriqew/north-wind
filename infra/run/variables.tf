variable "region" {
  type    = string
  default = "eu-north-1"
}

variable "bucket" {
  description = "The results bucket, from infra/storage"
  type        = string
}

variable "name" {
  description = "The window's name; the live run writes to runs/<name>-live"
  type        = string
}

variable "window_start" {
  description = "Start of the window, ISO-8601 UTC (2026-10-27T00:00:00Z)"
  type        = string
}

variable "window_end" {
  description = "End of the window, ISO-8601 UTC, exclusive"
  type        = string
}

# The three timers, as systemd calendar times in UTC ("2026-10-26 23:00:00 UTC").
variable "start_at" {
  description = "When the ingest, recorder and live job start"
  type        = string
}

variable "stop_at" {
  description = "When the job is drained, the recording closed and both copied to S3"
  type        = string
}

variable "poweroff_at" {
  description = "When the machine powers itself off, whatever state the run is in"
  type        = string
}

variable "instance_type" {
  type    = string
  default = "t4g.large"
}

variable "volume_gb" {
  type    = number
  default = 30
}

variable "compose_version" {
  type    = string
  default = "2.29.7"
}
