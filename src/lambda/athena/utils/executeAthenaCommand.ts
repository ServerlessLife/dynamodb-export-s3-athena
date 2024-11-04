import {
  AthenaClient,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";

const athenaClient = new AthenaClient({});
const glueDatabaseName = process.env.GLUE_DATABASE_NAME!;
const athenaWorkgroupName = process.env.ATHENA_WORK_GROUP_NAME!;

export async function executeAthenaCommand(
  command: string,
  parameters?: string[]
) {
  console.log(`Executing query: ${command}`);

  const startQueryExecutionCommand = new StartQueryExecutionCommand({
    QueryString: command,
    QueryExecutionContext: {
      Database: glueDatabaseName,
    },
    WorkGroup: athenaWorkgroupName,
    ExecutionParameters: parameters,
  });

  const { QueryExecutionId } = await athenaClient.send(
    startQueryExecutionCommand
  );
  console.log(`Query started with ID: ${QueryExecutionId}`);
  return { QueryExecutionId };
}
