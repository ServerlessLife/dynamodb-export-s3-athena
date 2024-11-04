import { Handler, S3Event } from "aws-lambda";
import { executeAthenaCommand } from "./utils/executeAthenaCommand";
import { extractExportedDynamoDbTablesFromEvent } from "./utils/extractExportedDynamoDbTablesFromEvent";
import { recreateAthenaTable } from "./utils/recreateAthenaTable";

const dynamoDBExportBucket = process.env.BUCKET_NAME!;

export const handler: Handler = async (event: S3Event) => {
  const exports = extractExportedDynamoDbTablesFromEvent(event);

  for (const exportDetail of exports) {
    const dynamoDBTableName = exportDetail.tableName;
    const exportId = exportDetail.exportId;

    if (dynamoDBTableName === "Item") {
      const athenaDropTableQuery = `DROP TABLE IF EXISTS item;`;
      const athenaCreateTableQuery = `
          CREATE EXTERNAL TABLE item (
              Item struct <itemId: string,
                           category: string,
                           name: string,
                           price: decimal(10,2)
                          >
          )
          ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
          LOCATION 's3://${dynamoDBExportBucket}/${dynamoDBTableName}/AWSDynamoDB/${exportId}/data/'
          TBLPROPERTIES ('has_encrypted_data'='true');
      `;

      await recreateAthenaTable({
        dropTableCommand: athenaDropTableQuery,
        createTableCommand: athenaCreateTableQuery,
      });
    } else if (dynamoDBTableName === "CustomerOrder") {
      const athenaDropTableQuery = `DROP TABLE IF EXISTS customer_order;`;
      const athenaCreateTableQuery = `
          CREATE EXTERNAL TABLE customer_order (
            Item struct <PK: string,
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
                        >
          )
          ROW FORMAT SERDE 'com.amazon.ionhiveserde.IonHiveSerDe'
          LOCATION 's3://${dynamoDBExportBucket}/${dynamoDBTableName}/AWSDynamoDB/${exportId}/data/'
          TBLPROPERTIES ('has_encrypted_data'='true');
      `;
      await recreateAthenaTable({
        dropTableCommand: athenaDropTableQuery,
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
            SELECT Item.customerId AS customer_id,
                  Item.name AS name,
                  Item.email AS email
            FROM "customer_order"
            WHERE Item.entity_type = 'CUSTOMER'
          ),
          "order" AS (
            SELECT Item.orderId AS order_id,
                  Item.customerId AS customer_id,
                  DATE(parse_datetime(Item.date, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z')) AS order_date
            FROM "customer_order"
            WHERE Item.entity_type = 'ORDER'
          ),
          "order_item" AS (
            SELECT Item.orderId AS order_id,
                  Item.itemId AS item_id,
                  Item.itemName AS item_name,
                  Item.price AS price,
                  Item.quantity AS quantity
            FROM "customer_order"
            WHERE Item.entity_type = 'ORDER_ITEM'
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
            SELECT Item.customerId AS customer_id,
                   Item.name AS name,
                   Item.email AS email
            FROM "customer_order"
            WHERE Item.entity_type = 'CUSTOMER'
          ),
          "order" AS (
            SELECT Item.orderId AS order_id,
                   Item.customerId AS customer_id,
                   DATE(parse_datetime(Item.date, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z')) AS order_date
            FROM "customer_order"
            WHERE Item.entity_type = 'ORDER'
          ),
          "order_item" AS (
            SELECT Item.orderId AS order_id,
                   Item.itemId AS item_id,
                   Item.itemName AS item_name,
                   Item.price AS price,
                   Item.quantity AS quantity
            FROM "customer_order"
            WHERE Item.entity_type = 'ORDER_ITEM'
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
          SELECT COUNT(Item.Itemid) AS total_items
            FROM item
          `
      );
    }
  }
};
