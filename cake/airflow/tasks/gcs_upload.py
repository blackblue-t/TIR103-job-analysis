from google.cloud import storage

def upload_to_gcs(bucket_name, local_file_path, gcs_file_path):
    """上傳檔案到 GCS"""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_file_path)

    blob.upload_from_filename(local_file_path)
    print(f"檔案已成功上傳到 {bucket_name}/{gcs_file_path}.")