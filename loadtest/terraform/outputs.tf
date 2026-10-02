# `loadtest/loadtest inventory` reads these.

output "inventory" {
  value = {
    region   = var.region
    zone     = local.zone
    bucket   = aws_s3_bucket.data.bucket
    ssh_user = "ubuntu"
    nodes = [
      for index, instance in aws_instance.node : {
        name          = "node-${index}"
        public_ip     = instance.public_ip
        private_ip    = instance.private_ip
        instance_type = instance.instance_type
        standby       = index >= var.node_count
      }
    ]
    clients = [
      for index, instance in aws_instance.client : {
        name          = "client-${index}"
        public_ip     = instance.public_ip
        private_ip    = instance.private_ip
        instance_type = instance.instance_type
      }
    ]
  }
}

output "origin_access_key_id" {
  value = aws_iam_access_key.origin.id
}

output "origin_secret_access_key" {
  value     = aws_iam_access_key.origin.secret
  sensitive = true
}
