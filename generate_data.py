import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import random
import time
from faker import Faker
import io


# Define PyArrow Schema
schema = pa.schema([
    ("op", pa.string()),
    ("replicadmstimestamp", pa.timestamp('s')),
    ("invoiceid", pa.int64()),
    ("itemid", pa.int64()),
    ("category", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.int32()),
    ("orderdate", pa.date32()),
    ("destinationstate", pa.string()),
    ("shippingtype", pa.string()),
    ("referral", pa.string())
])


def generate_fake_data(num_records=100):
    """Generate a batch of fake records."""
    fake = Faker()
    data = [
        (
            random.choice(['I', 'U', 'D']),
            fake.date_time_this_year().timestamp(),  # Convert to UNIX timestamp
            fake.unique.random_number(digits=5),
            fake.unique.random_number(digits=3),  # Changed from 2 to 3 digits
            fake.word(),
            round(random.uniform(10, 100), 2),
            random.randint(1, 5),
            fake.date_this_decade().toordinal(),  # Convert to integer date
            fake.state_abbr(),
            random.choice(['2-Day', '3-Day', 'Standard']),
            fake.word()
        )
        for _ in range(num_records)
    ]
    return data


def write_parquet_to_memory(data):
    """Convert list of tuples into a Parquet file stored in memory."""
    table = pa.Table.from_arrays(list(zip(*data)), schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def upload_to_s3(bucket_name, s3_path, data_buffer):
    """Upload the Parquet file to S3."""
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=data_buffer.getvalue())
        print(f"Uploaded to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")


def generate_and_upload_data(bucket_name, num_files, num_records_per_file, s3_directory):
    """Generate fake data, convert to Parquet, and upload to S3."""
    for i in range(1, num_files + 1):
        # Reset the Faker unique state between files to avoid uniqueness errors
        Faker.seed(random.randint(1, 10000))
        data = generate_fake_data(num_records_per_file)
        parquet_buffer = write_parquet_to_memory(data)
        file_name = f"file{i}.parquet"
        s3_path = f"{s3_directory}/{file_name}"
        upload_to_s3(bucket_name, s3_path, parquet_buffer)
        print(f"Generated file {i}/{num_files} with {num_records_per_file} records")


if __name__ == "__main__":
    bucket_name = "XX"
    num_files = 10  # Number of Parquet files
    num_records_per_file = 500  # Number of records per file
    s3_directory = "raw"

    start_time = time.time()
    generate_and_upload_data(bucket_name, num_files, num_records_per_file, s3_directory)
    end_time = time.time()

    print(f"Total time taken: {end_time - start_time:.2f} seconds")
