# HMPPS DataHub Landing Zone Processing Lambda

## Summary

A lambda used for converting CSV files in the DataHub Landing Processing Zone to Parquet format in the Raw Zone.

The Lambda parses the lambda payload and then for every file below the provided prefix (folder markers and empty
objects are skipped, with a warning in the logs):
1. Read the file as a CSV into memory
2. Read the associated data contract avro schema
3. Make all fields in the schema nullable so nulls can be allowed through
4. Add special columns to the avro schema that are expected by the batch processing job
5. Convert the CSV file to Avro, also filling in the special columns as appropriate
6. Write the records to Parquet in the output bucket

If a Step Function token was included in the payload then the lambda will report success to the Step Function API.
For most problems during conversion, the CSV file will be moved to an area of the Violations bucket.


The lambda payload should be in this format (which is compatible with S3 object creation notifications), 
where the `stepFunctionToken` field is optional:

```json
{
  "stepFunctionToken": "token",
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "landing-processing-bucket-name"
        },
        "object": {
          "key": "path/to/prefix"
        }
      }
    }
  ]
}
```

`stepFunctionToken` is an optional token indicating that the lambda was invoked from a Step Function. If it is present
then the Lambda will report success to the Step Function API using this token when it succeeds.

The s3 bucket name is the name of the input bucket triggering the lambda to process. 
The s3 objkect key is the name of the prefix in S3 to process.

## Debugging a failed CSV file

`LocalCsvFileTest` runs a CSV file from your machine through the same reading and conversion code the lambda uses,
without needing AWS. Download the CSV file that failed, and its contract's Avro schema from the schema registry bucket
or data contracts repo, then run the following (or IDE equivalent):

```shell
LOCAL_CSV_PATH="/path/to/file.csv" \
LOCAL_SCHEMA_PATH="/path/to/contract.avsc" \
./gradlew test --rerun --tests '*LocalCsvFileTest' -i
```

It checks that the file can be read and that every row has the same number of columns, then converts it to Parquet
twice: once treating every column as a string, and once with the contract if `LOCAL_SCHEMA_PATH` is set. The `-i`
flag shows the lambda's log lines, which say which field couldn't be converted and why.

- The tests are skipped unless `LOCAL_CSV_PATH` is set, so they don't run in CI. The contract test is also skipped
  unless `LOCAL_SCHEMA_PATH` is set.
- `--rerun` is needed because Gradle doesn't treat environment variables as inputs, so without it, it can decide the
  tests are up to date and not run them.
- The tests read the file as UTF-8 with one header row. If the domain is configured differently, change
  `numberOfHeaderRowsToSkip` or the charset in the test.
- Keep the downloaded files outside the repo, so they aren't committed by mistake.
