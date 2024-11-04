#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { DynamoDbExportS3AthenaStack } from "../lib/dynamoDbExportS3AthenaStack";

const app = new cdk.App();
new DynamoDbExportS3AthenaStack(app, "DynamoDbExportFullAthena", {
  stackName: "dynamodb-export-full-athena",
  exportType: "FULL_EXPORT",
});

new DynamoDbExportS3AthenaStack(app, "DynamoDbExportIncrementalAthena", {
  stackName: "dynamodb-export-incremental-athena",
  exportType: "INCREMENTAL_EXPORT",
});
