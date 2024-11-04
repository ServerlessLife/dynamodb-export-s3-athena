import * as constructs from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glueA from "@aws-cdk/aws-glue-alpha";

export interface GlueDbProps {}

export class GlueDb extends constructs.Construct {
  public readonly databaseBucket: s3.Bucket;
  public readonly glueDb: glueA.Database;

  constructor(scope: constructs.Construct, id: string, props: GlueDbProps) {
    super(scope, id);

    this.databaseBucket = new s3.Bucket(this, "DatabaseBucket", {
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.glueDb = new glueA.Database(this, "GlueDatabase", {
      databaseName: `${cdk.Stack.of(
        this
      ).stackName.toLocaleLowerCase()}-database`,
    });
  }
}
