import { Handler, S3Event } from "aws-lambda";
import { executeAthenaCommand } from "./utils/executeAthenaCommand";
import { extractExportedDynamoDbTablesFromEvent } from "./utils/extractExportedDynamoDbTablesFromEvent";
import { createAthenaTable } from "./utils/createAthenaTable";

const dynamoDBExportBucket = process.env.BUCKET_NAME!;

export const handler: Handler = async (event: S3Event) => {
  const exports = extractExportedDynamoDbTablesFromEvent(event);

  for (const exportDetail of exports) {
    const dynamoDBTableName = exportDetail.tableName;

    if (dynamoDBTableName === "Item") {
      const athenaCreateTableQuery = `
          CREATE EXTERNAL TABLE item (
              Record struct <NewImage: struct <itemId: string,
                                               category: string,
                                               name: string,
                                               price: decimal(10,2)
                                              >,
                             OldImage: struct <itemId: string>
                            >
          )
          ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
          LOCATION 's3://${dynamoDBExportBucket}/${dynamoDBTableName}/AWSDynamoDB/data/'
          TBLPROPERTIES ('has_encrypted_data'='true');
      `;

      await createAthenaTable({
        createTableCommand: athenaCreateTableQuery,
      });
    } else if (dynamoDBTableName === "CustomerOrder") {
      const athenaCreateTableQuery = `
          CREATE EXTERNAL TABLE customer_order (
            Record struct <NewImage: struct <PK: string,
                                             SK: string,
                                             customerId: string,
                                             date: string,
                                             ENTITY_TYPE: string,
                                             orderId: string,
                                             email: string,
                                             name: string,
                                             itemId: string,
                                             itemName: string,
                                             price: decimal(10,2),
                                             quantity: decimal(10,0)
                                            >,
                           OldImage: struct <PK: string,
                                             SK: string>
                          >

          )
          ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
          LOCATION 's3://${dynamoDBExportBucket}/${dynamoDBTableName}/AWSDynamoDB/data/'
          TBLPROPERTIES ('has_encrypted_data'='true');
      `;
      await createAthenaTable({
        createTableCommand: athenaCreateTableQuery,
      });
    } else {
      throw new Error(`Unknown table name: ${dynamoDBTableName}`);
    }

    if (dynamoDBTableName === "CustomerOrder") {
      const today = new Date().toISOString().split("T")[0];

      // Total earnings each day
      await executeAthenaCommand(
        `
          WITH "customer" AS (
            SELECT DISTINCT
                   Record.NewImage.customerId AS customer_id,
                   Record.NewImage.name AS name,
                   Record.NewImage.email AS email
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'CUSTOMER'
          ),
          "order" AS (
            SELECT DISTINCT
                   Record.NewImage.orderId AS order_id,
                   Record.NewImage.customerId AS customer_id,
                   DATE(parse_datetime(Record.NewImage.date, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z')) AS order_date
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'ORDER'
          ),
          "order_item" AS (
            SELECT DISTINCT
                   Record.NewImage.orderId AS order_id,
                   Record.NewImage.itemId AS item_id,
                   Record.NewImage.itemName AS item_name,
                   Record.NewImage.price AS price,
                   Record.NewImage.quantity AS quantity
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'ORDER_ITEM'
          )
          SELECT o.order_date AS order_date,
                SUM(oi.price * oi.quantity) AS total
          FROM "order" AS o
          INNER JOIN "order_item" AS oi
              ON oi.order_id = o.order_id
          WHERE o.order_date = parse_datetime(?, 'yyyy-MM-dd')
          GROUP BY o.order_date
          ORDER BY order_date;
        `,
        [`'${today}'`]
      );

      // Most expensive order of the day with all ordered items
      await executeAthenaCommand(
        `
          WITH "customer" AS (
            SELECT DISTINCT
                   Record.NewImage.customerId AS customer_id,
                   Record.NewImage.name AS name,
                   Record.NewImage.email AS email
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'CUSTOMER'
          ),
          "order" AS (
            SELECT DISTINCT
                   Record.NewImage.orderId AS order_id,
                   Record.NewImage.customerId AS customer_id,
                   DATE(parse_datetime(Record.NewImage.date, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z')) AS order_date
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'ORDER'
          ),
          "order_item" AS (
            SELECT DISTINCT
                   Record.NewImage.orderId AS order_id,
                   Record.NewImage.itemId AS item_id,
                   Record.NewImage.itemName AS item_name,
                   Record.NewImage.price AS price,
                   Record.NewImage.quantity AS quantity
            FROM "customer_order"
            WHERE Record.NewImage.entity_type = 'ORDER_ITEM'
          )
          SELECT o.order_id,
                 c.customer_id,
                 c.name AS customer_name,
                 SUM(oi.price * oi.quantity) AS total,
                 ARRAY_AGG(
                   CAST(
                     CAST(
                       ROW(oi.item_id, oi.item_name, oi.quantity, oi.price)
                         AS ROW(item_id VARCHAR, item_name VARCHAR, quantity INTEGER, price DOUBLE)
                   ) AS JSON)
                 ) AS items
            FROM "order" AS o
                INNER JOIN "order_item" AS  oi
                        ON oi.order_id = o.order_id
                INNER JOIN "customer" AS c
                        ON c.customer_id = o.customer_id
          WHERE o.order_date = parse_datetime(?, 'yyyy-MM-dd')
          GROUP BY o.order_id, c.customer_id, c.name
          ORDER BY total DESC
          LIMIT 1

        `,
        [`'${today}'`]
      );
    }

    if (dynamoDBTableName === "Item") {
      // Total inventory count
      await executeAthenaCommand(
        `
          SELECT COUNT(DISTINCT i.Record.NewImage.itemId) AS total_items
            FROM "item" AS i
                LEFT OUTER JOIN (SELECT DISTINCT Record.OldImage.itemId as item_id
                                   FROM "item"
                                  WHERE Record.OldImage.itemId IS NOT NULL
                                ) AS i_deleted
                          ON i.Record.OldImage.itemId = i.Record.NewImage.itemId
          WHERE i.Record.NewImage.itemId IS NOT NULL
            AND i_deleted.item_id IS NULL
        `
      );
    }
  }
};
