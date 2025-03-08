

## Problem Statement
![image](https://github.com/user-attachments/assets/5494cc09-8b52-467c-b69c-313ed7d3afd5)

* When processing new files from an S3 bucket, many implementations rely on the LastModified timestamp to determine which files are new. However, this approach has several edge cases:

* Clock Skew: If files are copied or moved, the timestamp might not reflect the actual arrival time.

* Eventual Consistency: S3's eventual consistency model can cause delays, making some files appear out of order.

* Overwrite Issues: If a file is updated in place, the modified timestamp changes, leading to duplicate or missing records.


---------------


## Solution: Manifest-Based Incremental Processing

To ensure robust and reliable processing, we use a manifest-based approach:

List all files in the raw data folder.

Generate a manifest file with the file paths.

Process files in the manifest using Apache Spark.

Archive processed files to prevent reprocessing.

Handle failures gracefully by moving failed manifests to an error folder.


