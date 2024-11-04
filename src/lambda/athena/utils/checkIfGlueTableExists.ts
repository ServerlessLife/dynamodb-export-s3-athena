import { GlueClient, GetTableCommand } from "@aws-sdk/client-glue";

const glueClient = new GlueClient({});

export async function checkIfGlueTableExists(
  databaseName: string,
  tableName: string
): Promise<boolean> {
  try {
    const getTableCommand = new GetTableCommand({
      DatabaseName: databaseName,
      Name: tableName,
    });

    const response = await glueClient.send(getTableCommand);
    return !!response.Table;
  } catch (error: any) {
    if (error.name === "EntityNotFoundException") {
      return false; // Table does not exist
    } else {
      throw error; // Other errors should be handled separately
    }
  }
}
