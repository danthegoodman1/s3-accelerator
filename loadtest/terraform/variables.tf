variable "name" {
  description = "Prefixes every resource, and tags them all."
  type        = string
  default     = "s3accel-load"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "availability_zone" {
  description = "Every host runs in this zone, as a cluster serves one zone. Empty takes the first of the region's standard zones that offers both instance types."
  type        = string
  default     = ""
}

variable "operator_cidr" {
  description = "Addresses that may reach the hosts over SSH, such as your own address with /32."
  type        = string
}

variable "ssh_public_key" {
  description = "The public key SSH logins take."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "node_count" {
  description = "Storage nodes that start with the cluster."
  type        = number
  default     = 4
}

variable "standby_count" {
  description = "Storage nodes prepared but left stopped, which a plan's faults start to grow the cluster."
  type        = number
  default     = 1
}

variable "node_type" {
  description = "An instance type with local NVMe instance storage."
  type        = string
  default     = "i4i.4xlarge"
}

variable "client_count" {
  description = "Hosts that run the load generator, and a gateway each when gateways run on clients."
  type        = number
  default     = 4
}

variable "client_type" {
  type    = string
  default = "c6in.8xlarge"
}

variable "ami" {
  description = "An x86_64 AMI with Linux 7.0 or later, for kernel TLS. Empty takes the latest Ubuntu 26.04."
  type        = string
  default     = ""
}

variable "cluster_placement" {
  description = "Places every host in a cluster placement group, for the network's full bandwidth between them. A group can run out of capacity for large instance types."
  type        = bool
  default     = false
}
