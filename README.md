# HMPPS DataHub Landing Zone Processing Lambda

## Summary

A lambda used for converting CSV files in the DataHub Landing Processing Zone to Parquet format in the Raw Zone.

The Lambda parses the lambda payload and then for every file below the provided prefix:
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
