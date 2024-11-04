import { AthenaClient, GetQueryExecutionCommand } from "@aws-sdk/client-athena";
import { setTimeout } from "timers/promises";
import { executeAthenaCommand } from "./executeAthenaCommand";

const athenaClient = new AthenaClient({});

export async function executeAthenaCommandAndWait(
  command: string,
  parameters?: string[]
) {
  const response = await executeAthenaCommand(command, parameters);

  // Wait in a loop until query execution is done
  // (use only for short-running queries)
  let status: string | undefined;
  do {
    const queryStatusCommand = new GetQueryExecutionCommand({
      QueryExecutionId: response.QueryExecutionId,
    });
    const queryExecutionStatus = await athenaClient.send(queryStatusCommand);
    status = queryExecutionStatus.QueryExecution?.Status?.State;
    if (status === "QUEUED" || status === "RUNNING") {
      await setTimeout(2000);
    }
  } while (status === "QUEUED" || status === "RUNNING");

  if (status !== "SUCCEEDED") {
    throw new Error(`Command ${command} failed with status: ${status}`);
  }
}
