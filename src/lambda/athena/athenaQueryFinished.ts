import {
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  QueryExecutionState,
} from "@aws-sdk/client-athena";
import { EventBridgeHandler } from "aws-lambda";
import { AthenaClient } from "@aws-sdk/client-athena";
import { transformAthenaResults } from "./utils/transformAthenaResults";

const athena = new AthenaClient({});

export const handler: EventBridgeHandler<
  "athena.query_execution_state_change",
  {
    queryExecutionId: string;
    currentState: QueryExecutionState;
    athenaError: {
      errorMessage: string;
    };
  },
  void
> = async (event) => {
  const { queryExecutionId, currentState } = event.detail;

  if (currentState === QueryExecutionState.FAILED) {
    throw new Error(
      `Athana query failed. Error: ${event.detail.athenaError.errorMessage}`
    );
  }

  const getQueryExecutionCommand = new GetQueryExecutionCommand({
    QueryExecutionId: queryExecutionId,
  });

  const queryInfo = await athena.send(getQueryExecutionCommand);

  let nextToken: string | undefined;
  let allResults: any[] = [];

  do {
    const commandGetQueryResultsCommand = new GetQueryResultsCommand({
      QueryExecutionId: queryExecutionId,
      NextToken: nextToken,
    });

    const responseResults = await athena.send(commandGetQueryResultsCommand);

    nextToken = responseResults.NextToken;

    allResults = [
      ...allResults,
      ...transformAthenaResults(responseResults.ResultSet),
    ];
  } while (nextToken);

  console.log("********************** QUERY ***********************");
  console.log(queryInfo.QueryExecution?.Query);
  console.log("******************* * RESULTS **********************");
  console.log(JSON.stringify(allResults, null, 2));
  console.log("****************************************************");
};
