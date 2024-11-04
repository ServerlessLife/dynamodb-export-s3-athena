import * as constructs from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as athena from "aws-cdk-lib/aws-athena";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";

export interface AthenaWorkGroupProps {
  readonly glueDatabaseName: string;
  readonly databaseBucket: s3.Bucket;
}

export class AthenaWorkGroup extends constructs.Construct {
  public athenaWorkGroup: athena.CfnWorkGroup;
  public athenaQueryResultBucket: s3.Bucket;
  private readonly glueDatabaseName: string;
  private readonly databaseBucket: s3.Bucket;
  constructor(
    scope: constructs.Construct,
    id: string,
    props: AthenaWorkGroupProps
  ) {
    super(scope, id);
    this.glueDatabaseName = props.glueDatabaseName;
    this.databaseBucket = props.databaseBucket;

    this.athenaQueryResultBucket = new s3.Bucket(this, "AggregationBucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.athenaWorkGroup = new athena.CfnWorkGroup(this, "AthenaWorkGroup", {
      // add cdk stack name to avoid name conflict
      name: `${cdk.Stack.of(this).stackName}-AthenaWorkGroup`,
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        resultConfiguration: {
          outputLocation: this.athenaQueryResultBucket.s3UrlForObject(),
          encryptionConfiguration: {
            encryptionOption: "SSE_S3",
          },
        },
      },
    });
  }

  public grantQueryExecution(func: lambda.Function) {
    func.role?.attachInlinePolicy(
      new iam.Policy(this, `${func.node.id}AthenaQueryExecutionPolicy`, {
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
              "athena:StartQueryExecution",
              "athena:GetQueryExecution",
              "athena:GetQueryResults",
              "athena:StopQueryExecution",
            ],
            resources: [
              `arn:aws:athena:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:workgroup/${this.athenaWorkGroup.name}`,
            ],
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ["glue:GetTable"],
            resources: [
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:catalog`,
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:database/${this.glueDatabaseName}`,
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:table/${this.glueDatabaseName}/*`,
            ],
          }),
        ],
      })
    );
    this.databaseBucket.grantRead(func);
    this.athenaQueryResultBucket.grantReadWrite(func);
  }

  public grantReadQueryResults(func: lambda.Function) {
    func.role?.attachInlinePolicy(
      new iam.Policy(this, `${func.node.id}AthenaReadQueryResultsPolicy`, {
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ["athena:GetQueryExecution", "athena:GetQueryResults"],
            resources: [
              `arn:aws:athena:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:workgroup/${this.athenaWorkGroup.name}`,
            ],
          }),
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ["glue:GetTable"],
            resources: [
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:catalog`,
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:database/${this.glueDatabaseName}`,
              `arn:aws:glue:${cdk.Stack.of(this).region}:${
                cdk.Stack.of(this).account
              }:table/${this.glueDatabaseName}/*`,
            ],
          }),
        ],
      })
    );
    this.databaseBucket.grantRead(func);
    this.athenaQueryResultBucket.grantRead(func);
  }
}
