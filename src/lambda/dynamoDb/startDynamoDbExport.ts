import { Handler } from "aws-lambda";
import {
  DescribeContinuousBackupsCommand,
  DynamoDBClient,
  ExportFormat,
  ExportTableToPointInTimeCommand,
  ExportType,
} from "@aws-sdk/client-dynamodb";

const dynamoDBClient = new DynamoDBClient({});
const exportType = process.env.EXPORT_TYPE! as ExportType;

export const handler: Handler = async () => {
  await startExport({
    tableArn: process.env.CUSTOMER_ORDER_TABLE_ARN!,
    bucketName: process.env.BUCKET_NAME!,
    tableName: `CustomerOrder`,
    exportType,
  });

  await startExport({
    tableArn: process.env.ITEM_TABLE_ARN!,
    bucketName: process.env.BUCKET_NAME!,
    tableName: `Item`,
    exportType,
  });
};

async function startExport({
  tableArn,
  bucketName,
  tableName,
  exportType,
}: {
  tableArn: string;
  bucketName: string;
  tableName: string;
  exportType: ExportType;
}) {
  let exportFromTime: Date | undefined;
  let exportToTime: Date | undefined;

  // if incremental export, set the exportFromTime from midnight
  // You might want to adjust this based on your use case.
  // Note that you can not export more than 24 hours of data in a single export.
  if (exportType === ExportType.INCREMENTAL_EXPORT) {
    exportFromTime = new Date();
    exportFromTime.setUTCHours(0, 0, 0, 0);

    const command = new DescribeContinuousBackupsCommand({
      // extract table name from tableArn
      TableName: tableArn.split("/").pop()!,
    });
    const response = await dynamoDBClient.send(command);

    const earliestDateTime =
      response?.ContinuousBackupsDescription?.PointInTimeRecoveryDescription
        ?.EarliestRestorableDateTime;

    if (earliestDateTime) {
      // if exportFromTime is earlier than Earliest Restorable Time, set it to Earliest Restorable Time
      if (exportFromTime < earliestDateTime) {
        exportFromTime = earliestDateTime;
      }
    }

    exportToTime = new Date();

    console.log(`Incremental export from ${exportFromTime} to ${exportToTime}`);
  }

  let exportCommand = new ExportTableToPointInTimeCommand({
    TableArn: tableArn,
    S3Bucket: bucketName,
    S3Prefix: `${tableName}/`,
    ExportType: exportType,
    ExportFormat: ExportFormat.ION,
    IncrementalExportSpecification:
      exportType === ExportType.INCREMENTAL_EXPORT
        ? {
            ExportFromTime: exportFromTime,
            ExportToTime: exportToTime,
          }
        : undefined,
  });

  let exportResponse = await dynamoDBClient.send(exportCommand);
  console.log(
    `Table ${tableName} export triggered`,
    JSON.stringify(exportResponse)
  );
  return { exportCommand, exportResponse };
}
