import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { AthenaWorkGroup } from "./athenaWorkGroup";
import { GlueDb } from "./glueDb";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda_node from "aws-cdk-lib/aws-lambda-nodejs";
import * as events from "aws-cdk-lib/aws-events";
import * as events_targets from "aws-cdk-lib/aws-events-targets";
import * as logs from "aws-cdk-lib/aws-logs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";

export interface DynamoDbExportS3AthenaStackProps extends cdk.StackProps {
  readonly exportType: "FULL_EXPORT" | "INCREMENTAL_EXPORT";
}

export class DynamoDbExportS3AthenaStack extends cdk.Stack {
  constructor(
    scope: Construct,
    id: string,
    props: DynamoDbExportS3AthenaStackProps
  ) {
    super(scope, id, props);

    // ****************************** DynamoDB tables **************************************
    const tableCutomerOrder = new dynamodb.TableV2(this, "CustomerOrder", {
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
      dynamoStream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    const tableItem = new dynamodb.TableV2(this, "Item", {
      partitionKey: { name: "itemId", type: dynamodb.AttributeType.STRING },
      dynamoStream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    // ************************* Glue schemas ************************
    const { databaseBucket, glueDb } = new GlueDb(this, "GlueDb", {});

    // ************************* Athena workgroup **************************
    const athenaWorkGroup = new AthenaWorkGroup(this, "AthenaWorkGroup", {
      glueDatabaseName: glueDb.databaseName,
      databaseBucket,
    });

    // ********************************* Functions **************************************
    const bundling: lambda_node.BundlingOptions = {
      format: lambda_node.OutputFormat.ESM,
      sourceMap: true,
      banner:
        'import { createRequire } from "module";const require = createRequire(import.meta.url);',
      tsconfig: "src/tsconfig.json",
    };

    // startDynamoDbExport
    const startDynamoDbExportFunction = new lambda_node.NodejsFunction(
      this,
      "StartDynamoDbExportFunc",
      {
        entry: "src/lambda/dynamoDb/startDynamoDbExport.ts",
        runtime: lambda.Runtime.NODEJS_20_X,
        memorySize: 1024,
        timeout: cdk.Duration.seconds(10),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling,
        environment: {
          CUSTOMER_ORDER_TABLE_ARN: tableCutomerOrder.tableArn,
          ITEM_TABLE_ARN: tableItem.tableArn,
          BUCKET_NAME: databaseBucket.bucketName,
          EXPORT_TYPE: props.exportType,
        },
      }
    );

    startDynamoDbExportFunction.role?.attachInlinePolicy(
      new iam.Policy(this, "DynamoDBExportPolicy", {
        statements: [
          new iam.PolicyStatement({
            actions: ["dynamodb:ExportTableToPointInTime"],
            resources: [tableCutomerOrder.tableArn, tableItem.tableArn],
          }),
        ],
      })
    );
    databaseBucket.grantWrite(startDynamoDbExportFunction);

    if (props.exportType === "INCREMENTAL_EXPORT") {
      startDynamoDbExportFunction.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ["dynamodb:DescribeContinuousBackups"],
          resources: [tableCutomerOrder.tableArn, tableItem.tableArn],
        })
      );
    }

    const athenaQueryStartFunction = new lambda_node.NodejsFunction(
      this,
      "AthenaQueryStartFunc",
      {
        entry:
          props.exportType === "FULL_EXPORT"
            ? "src/lambda/athena/athenaQueryStartFullExport.ts"
            : "src/lambda/athena/athenaQueryStartIncrementalExport.ts",
        runtime: lambda.Runtime.NODEJS_20_X,
        memorySize: 1024,
        timeout: cdk.Duration.seconds(10),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling,
        environment: {
          BUCKET_NAME: databaseBucket.bucketName,
          GLUE_DATABASE_NAME: glueDb.databaseName,
          ATHENA_WORK_GROUP_NAME: athenaWorkGroup.athenaWorkGroup.name,
          NODE_OPTIONS: "--enable-source-maps",
          EXPORT_TYPE: props.exportType,
        },
      }
    );
    athenaWorkGroup.grantQueryExecution(athenaQueryStartFunction);
    athenaQueryStartFunction.role?.attachInlinePolicy(
      new iam.Policy(this, "GlueCreateTable", {
        statements: [
          new iam.PolicyStatement({
            actions: [
              "glue:GetDatabase",
              "glue:DeleteTable",
              "glue:CreateTable",
            ],
            resources: [glueDb.databaseArn, glueDb.catalogArn],
          }),
          new iam.PolicyStatement({
            actions: ["glue:CreateTable", "glue:DeleteTable"],
            resources: [`arn:aws:glue:${this.region}:${this.account}:table/*`],
          }),
        ],
      })
    );

    const athenaQueryFinishedFunction = new lambda_node.NodejsFunction(
      this,
      "AthenaQueryFinishedFunc",
      {
        entry: "src/lambda/athena/athenaQueryFinished.ts",
        runtime: lambda.Runtime.NODEJS_20_X,
        memorySize: 1024,
        timeout: cdk.Duration.minutes(5),
        logRetention: logs.RetentionDays.ONE_DAY,
        bundling,
        environment: {
          NODE_OPTIONS: "--enable-source-maps",
        },
      }
    );
    athenaWorkGroup.grantReadQueryResults(athenaQueryFinishedFunction);
    athenaQueryFinishedFunction.role?.attachInlinePolicy(
      new iam.Policy(this, "GlueGetDatabase", {
        statements: [
          new iam.PolicyStatement({
            actions: ["glue:GetDatabase"],
            resources: [glueDb.databaseArn],
          }),
        ],
      })
    );

    // ************************* Cron jobs **************************
    // Start DynamoDB export to S3 once a day at 20:00 UTC
    //  > For real-world use cases, you might want to start after meednight.
    //  > Take into account time zones, etc..
    //  > In that case you should also adjust queries to use yesterday's date.

    /* uncomment to enable */
    new events.Rule(this, "DynamoDBExportRule", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "20",
      }),
      targets: [new events_targets.LambdaFunction(startDynamoDbExportFunction)],
    });

    // ************************** EventBridge ************************

    // subscribe to finished DynamoDB export event
    databaseBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(athenaQueryStartFunction),
      { suffix: "manifest-files.json" }
    );

    // subscribe to finished Athena query event
    new events.Rule(this, "AthenaQueryFinishedRule", {
      eventPattern: {
        source: ["aws.athena"],
        detailType: ["Athena Query State Change"],
        detail: {
          currentState: ["FAILED", "SUCCEEDED"],
        },
      },
      targets: [new events_targets.LambdaFunction(athenaQueryFinishedFunction)],
    });

    // ************************* Outputs **************************
    new cdk.CfnOutput(this, "DynamoDBTableCustomerOrder", {
      value: tableCutomerOrder.tableName,
    });

    new cdk.CfnOutput(this, "DynamoDBTableItem", {
      value: tableItem.tableName,
    });
  }
}
