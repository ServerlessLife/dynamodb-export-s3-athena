import { executeAthenaCommandAndWait } from "./executeAthenaCommandAndWait";
import { extractTableFromCommand } from "./extractTableFromCommand";
import { checkIfGlueTableExists } from "./checkIfGlueTableExists";

const glueDatabaseName = process.env.GLUE_DATABASE_NAME!;

export async function createAthenaTable({
  createTableCommand,
}: {
  createTableCommand: string;
}) {
  const tableName = extractTableFromCommand(createTableCommand);

  const tableExists = await checkIfGlueTableExists(glueDatabaseName, tableName);

  if (!tableExists) {
    console.log(`Creating Athena table ${tableName}.`);
    await executeAthenaCommandAndWait(createTableCommand);
    console.log(`Athena table ${tableName} created.`);
  }
}
