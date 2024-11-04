import { S3Event } from "aws-lambda";

export function extractExportedDynamoDbTablesFromEvent(event: S3Event) {
  const details = event.Records.map((record) => {
    const s3ObjectKey = record.s3.object.key;

    // Extract the table name and exportId using regex
    const matches = s3ObjectKey.match(/^([^/]+)\/AWSDynamoDB\/([^/]+)\//);
    if (matches) {
      const tableName = matches[1];
      const exportId = matches[2];
      return { tableName, exportId };
    }

    return null;
  });

  // Filter out any null values
  return details.filter((detail) => detail !== null);
}
