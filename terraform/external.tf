#create detaflow metadata
resource "null_resource" "bucket_megalista_metadata" {
  provisioner "local-exec" {
    command = "(cd ..; sh ./terraform/scripts/deploy_cloud.sh ${data.google_client_config.current.project} ${var.bucket_name} ${var.region})"
  }

  depends_on = [google_storage_bucket.my_storage]
}
