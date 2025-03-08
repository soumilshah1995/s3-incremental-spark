import time
import boto3
import os
from pyspark.sql import SparkSession
import concurrent.futures
from functools import partial


class S3FileProcessor:
    def __init__(self, data_path, pending_prefix, archived_prefix, dev=True):
        """
        Initialize the S3FileProcessor class.

        :param data_path: Full S3 path to the raw data folder.
        :param pending_prefix: Prefix for pending files.
        :param archived_prefix: Prefix for archived files.
        :param dev: Boolean flag to use 's3a://' for Spark or 's3://' for boto3.
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name, self.raw_prefix = self._parse_s3_path(data_path)
        self.pending_prefix = pending_prefix
        self.archived_prefix = archived_prefix
        self.dev = dev
        self.uri_scheme = "s3a://" if dev else "s3://"
        self.spark = self._create_spark_session()

    def _parse_s3_path(self, s3_path):
        """
        Parse an S3 path into bucket name and prefix.

        :param s3_path: Full S3 path
        :return: Tuple of (bucket_name, prefix)
        """
        path_without_scheme = s3_path.split("://", 1)[-1]
        parts = path_without_scheme.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    def _create_spark_session(self):
        """
        Create and return a Spark session configured for S3A.

        :return: SparkSession object.
        """
        return SparkSession.builder \
            .appName("S3FileProcessor") \
            .config("spark.jars.packages",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.catalog.s3tablesbucket.client.region", "us-east-1") \
            .config("spark.sql.catalog.dev.s3.endpoint", "https://s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

    def create_pending_manifest(self, max_files=1000):
        """
        List files in the raw prefix and create a manifest file in the pending folder.

        :param max_files: Maximum number of files to include in the manifest.
        :return: Path to the manifest file, or None if no files were found.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.raw_prefix)

        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                files.append(f"{self.uri_scheme}{self.bucket_name}/{obj['Key']}")
                if len(files) >= max_files:
                    break
            if len(files) >= max_files:
                break

        if not files:
            print("No files found in the raw prefix. Skipping manifest creation.")
            return None

        manifest_content = '\n'.join(files)
        manifest_key = f"{self.pending_prefix}manifest.pending"

        # Write manifest file to S3
        self.s3_client.put_object(Bucket=self.bucket_name, Key=manifest_key, Body=manifest_content)

        print(f"Manifest file created: {self.uri_scheme}{self.bucket_name}/{manifest_key}")

        return f"{self.uri_scheme}{self.bucket_name}/{manifest_key}"

    def process_pending_files(self, manifest_path):
        """
        Read and process files listed in the manifest using Spark.

        :param manifest_path: Path to the manifest file.
        :return: Processed Spark DataFrame.
        """
        # Read the manifest file as text
        manifest_df = self.spark.read.text(manifest_path)

        # Extract file paths from the manifest and read them as Parquet
        data_df = self.spark.read.parquet(*manifest_df.rdd.map(lambda r: r[0]).collect())

        # Process and display data
        print("Number of rows:", data_df.count())
        print("Schema:")
        data_df.printSchema()
        time.sleep(15)

        return data_df

    def archive_processed_files(self, manifest_path):
        """
        Archive processed files by moving them from raw to archived prefix using threading.

        :param manifest_path: Path to the manifest file.
        """
        # Read the content of the manifest file from S3
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=manifest_path.split('/', 3)[-1])
        content = response['Body'].read().decode('utf-8')

        file_paths = [path.strip() for path in content.split('\n') if path.strip()]

        # Create a partial function with pre-filled arguments
        archive_file = partial(self._archive_single_file, bucket_name=self.bucket_name,
                               archived_prefix=self.archived_prefix)

        # Use ThreadPoolExecutor to parallelize the archiving process
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(archive_file, file_path) for file_path in file_paths]

            for future in concurrent.futures.as_completed(futures):
                try:
                    old_key, new_key = future.result()
                    print(f"Archived: {old_key} -> {new_key}")
                except Exception as e:
                    print(f"Error archiving file: {str(e)}")

        # Delete the manifest file after archiving
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=manifest_path.split('/', 3)[-1])

    def _archive_single_file(self, file_path, bucket_name, archived_prefix):
        """
        Archive a single file.

        :param file_path: Path of the file to archive.
        :param bucket_name: Name of the S3 bucket.
        :param archived_prefix: Prefix for archived files.
        :return: Tuple of (old_key, new_key)
        """
        old_key = file_path.split('/', 3)[-1]
        new_key = f"{archived_prefix}{os.path.basename(old_key)}"

        # Copy object to archive folder and delete from raw folder
        self.s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=new_key
        )
        self.s3_client.delete_object(Bucket=bucket_name, Key=old_key)

        return old_key, new_key

    def cleanup_pending_manifest(self, manifest_path, move_to_error=True):
        """
        Clean up pending manifest by moving to error folder or deleting.

        :param manifest_path: Full S3 path to manifest file
        :param move_to_error: Whether to preserve in error folder (default True)
        """
        # Parse manifest path
        path_without_scheme = manifest_path.split("://", 1)[-1]
        bucket_name = path_without_scheme.split("/")[0]
        manifest_key = "/".join(path_without_scheme.split("/")[1:])

        if move_to_error:
            error_key = f"error/{manifest_key.split('/')[-1]}"
            self.s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': manifest_key},
                Key=error_key
            )
            print(f"Moved failed manifest to: s3://{bucket_name}/{error_key}")

        # Always delete from pending location
        self.s3_client.delete_object(
            Bucket=bucket_name,
            Key=manifest_key
        )


def main():
    """
    Main function demonstrating usage of S3FileProcessor class.
    """
    data_path = 's3://XXX5/raw/'
    pending_prefix = 'pending/'
    archived_prefix = 'archived/'
    max_files = 5

    # Initialize processor with dev flag set to True (for Spark compatibility)
    processor = S3FileProcessor(data_path, pending_prefix, archived_prefix, dev=True)

    # Step 1: Create a pending manifest
    manifest_path = processor.create_pending_manifest(max_files=max_files)

    if manifest_path is None:
        print("No files to process. Exiting.")
        return

    try:
        # Step 2: Process files listed in the pending manifest using Spark
        processed_data_df = processor.process_pending_files(manifest_path)
        processor.archive_processed_files(manifest_path)
    except Exception as e:
        print(f"Processing failed: {str(e)}")
        processor.cleanup_pending_manifest(manifest_path)
        raise  # Re-raise if you want to propagate the error
    finally:
        processor.spark.stop()


if __name__ == "__main__":
    main()
