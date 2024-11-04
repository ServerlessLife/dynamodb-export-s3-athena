import { type ResultSet } from "@aws-sdk/client-athena";

export function transformAthenaResults<T = any>(resultSet?: ResultSet) {
  const rows: T[] = [];

  if (resultSet?.Rows != null) {
    const dataRows = resultSet.Rows.slice(1);
    for (const resultStateRow of dataRows) {
      const row: any = {};
      if (resultStateRow.Data != null) {
        for (let i = 0; i < resultStateRow.Data.length; i++) {
          if (resultSet.ResultSetMetadata?.ColumnInfo != null) {
            const columnInfo = resultSet.ResultSetMetadata?.ColumnInfo[i];

            // transfrom stanke case to camel case
            if (columnInfo.Name) {
              const columnName = toCamelCase(columnInfo.Name);

              const columnValue = resultStateRow.Data[i].VarCharValue;

              if (columnValue) {
                switch (columnInfo.Type) {
                  case "boolean":
                    row[columnName] = columnValue === "true";
                    break;
                  case "integer":
                  case "tinyint":
                  case "smallint":
                    row[columnName] = parseInt(columnValue);
                    break;
                  case "bigint":
                    row[columnName] = parseFloat(columnValue);
                    break;
                  case "double":
                  case "float":
                  case "decimal":
                    row[columnName] = parseFloat(columnValue);
                    break;
                  case "char":
                  case "varchar":
                  case "string":
                    row[columnName] = columnValue;
                    break;
                  case "date":
                  case "timestamp":
                    row[columnName] = new Date(columnValue).toISOString();
                    break;
                  case "array":
                    row[columnName] = JSON.parse(columnValue);
                    break;
                  default:
                    throw new Error(
                      `Unknown column type ${columnInfo.Type ?? ""}`
                    );
                }
              } else {
                row[columnName] = undefined;
              }
            }
          }
        }
      }
      rows.push(row);
    }
  }
  return rows;
}

function toCamelCase(str: string) {
  return str
    .replace(/[^a-z0-9]/gi, " ")
    .toLowerCase()
    .split(" ")
    .map((el, ind) =>
      ind === 0 ? el : el[0].toUpperCase() + el.substring(1, el.length)
    )
    .join("");
}
