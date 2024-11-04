import { executeAthenaCommandAndWait } from "./executeAthenaCommandAndWait";
import { extractTableFromCommand } from "./extractTableFromCommand";

export async function recreateAthenaTable({
  dropTableCommand,
  createTableCommand,
}: {
  dropTableCommand: string;
  createTableCommand: string;
}) {
  const tableName = extractTableFromCommand(createTableCommand);
  console.log(`Recreating Athena table ${tableName}.`);
  await executeAthenaCommandAndWait(dropTableCommand);
  console.log(`Athena table ${tableName} dropped.`);
  await executeAthenaCommandAndWait(createTableCommand);
  console.log(`Athena table ${tableName} created.`);
}
