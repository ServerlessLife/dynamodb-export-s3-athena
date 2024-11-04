export function extractTableFromCommand(createTableCommand: string) {
  let tableRegex = /EXISTS (\w+)/i;
  let match = createTableCommand.match(tableRegex);
  if (!match) {
    tableRegex = /TABLE (\w+)/i;
    match = createTableCommand.match(tableRegex);
    if (!match) {
      throw new Error(
        "Unable to extract table name from create table command."
      );
    }
  }
  return match[1];
}
