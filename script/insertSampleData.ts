/**********************************************************************
 * Description: This script is used to insert sample data into the database
 **********************************************************************/

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { Customer } from "../src/types/customer";
import { faker } from "@faker-js/faker";
import { Order } from "../src/types/order";
import { Item } from "../src/types/item";
import { ItemCategory } from "../src/types/itemCategory";
import { OrderItem } from "../src/types/orderItem";
import { EntityType } from "../src/types/entityType";
import * as fs from "fs/promises";
import * as path from "path";
import * as crypto from "crypto";

async function run() {
  /********************** Get table names from stack output **********************/
  // get table names
  const cdkOutputs = JSON.parse(
    await fs.readFile(path.join("..", "cdk-outputs.json"), "utf-8")
  );

  //for each stack, get the output values
  for (const stackName in cdkOutputs) {
    const outputValues = cdkOutputs[stackName];
    await insertDataForStack(outputValues);
  }
}

async function insertDataForStack(outputValues: { [key: string]: string }) {
  const dynamoDBTableCustomerOrderName =
    outputValues["DynamoDBTableCustomerOrder"];

  const dynamoDBTableItemName = outputValues["DynamoDBTableItem"];

  const documentClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));

  for (let i = 0; i < 50; i++) {
    await insertSampleData();
  }

  async function insertSampleData() {
    const now = new Date().toISOString();
    //create some sample data
    const customer: Customer = {
      customerId: crypto.randomUUID(),
      name: faker.person.fullName(),
      email: faker.internet.email(),
      date: now,
    };

    const categories = Object.values(ItemCategory);
    const randomIndex = Math.floor(Math.random() * categories.length);

    const item1: Item = {
      itemId: crypto.randomUUID(),
      name: faker.commerce.productName(),
      price: parseFloat(faker.commerce.price({ min: 1, max: 200 })),
      category: categories[randomIndex],
    };

    const item2: Item = {
      itemId: crypto.randomUUID(),
      name: faker.commerce.productName(),
      price: parseFloat(faker.commerce.price({ min: 1, max: 200 })),
      category: categories[randomIndex],
    };

    const item3: Item = {
      itemId: crypto.randomUUID(),
      name: faker.commerce.productName(),
      price: parseFloat(faker.commerce.price({ min: 1, max: 200 })),
      category: categories[randomIndex],
    };

    const orderId = crypto.randomUUID();

    const orderItems: OrderItem[] = [
      {
        orderId,
        itemId: item1.itemId,
        itemName: item1.name,
        quantity: Math.floor(Math.random() * 10) + 1,
        price: item1.price,
        date: now,
      },
      {
        orderId,
        itemId: item2.itemId,
        itemName: item2.name,
        quantity: Math.floor(Math.random() * 10) + 1,
        price: item2.price,
        date: now,
      },
    ];

    const order: Order = {
      orderId,
      customerId: customer.customerId,
      items: orderItems,
      date: now,
    };

    await insertCustomerOrder({ customer, order, orderItems });

    await insertItems([item1, item2, item3]);

    // update item1 and item3
    item2.price = parseFloat(faker.commerce.price({ min: 1, max: 200 }));
    item3.price = parseFloat(faker.commerce.price({ min: 1, max: 200 }));

    await updateItem(item2);
    await updateItem(item3);

    // delete item3
    await deleteItem(item3.itemId);
  }

  async function insertCustomerOrder({
    customer,
    order,
    orderItems,
  }: {
    customer: Customer;
    order: Order;
    orderItems: OrderItem[];
  }) {
    const transactWriteCommand = new TransactWriteCommand({
      TransactItems: [
        {
          Put: {
            Item: {
              PK: `CUSTOMER#${customer.customerId}`,
              SK: `CUSTOMER#${customer.customerId}`,
              ...customer,
              ENTITY_TYPE: EntityType.CUSTOMER,
            },
            TableName: dynamoDBTableCustomerOrderName,
          },
        },
        {
          Put: {
            Item: {
              PK: `CUSTOMER#${customer.customerId}`,
              SK: `ORDER#${order.orderId}`,
              orderId: order.orderId,
              customerId: customer.customerId,
              date: order.date,
              ENTITY_TYPE: EntityType.ORDER,
            },
            TableName: dynamoDBTableCustomerOrderName,
          },
        },
        ...orderItems.map((orderItem) => ({
          Put: {
            Item: {
              PK: `ORDER#${order.orderId}`,
              SK: `ORDER_ITEM#${orderItem.itemId}`,
              ...orderItem,
              ENTITY_TYPE: EntityType.ORDER_ITEM,
            },
            TableName: dynamoDBTableCustomerOrderName,
          },
        })),
      ],
    });

    const r = await documentClient.send(transactWriteCommand);
  }

  async function insertItems(items: Item[]) {
    const batchWriteCommand = new BatchWriteCommand({
      RequestItems: {
        [dynamoDBTableItemName!]: items.map((item) => ({
          PutRequest: {
            Item: item,
          },
        })),
      },
    });

    const r = await documentClient.send(batchWriteCommand);

    if (r.UnprocessedItems?.[dynamoDBTableItemName!]) {
      console.error("Unprocessed items while inserting", r.UnprocessedItems);
    }
  }

  async function updateItem(item: Item) {
    const updateCommand = new UpdateCommand({
      TableName: dynamoDBTableItemName!,
      Key: {
        itemId: item.itemId,
      },
      UpdateExpression: `SET
      #price = :price,
      #name = :name,
      #category = :category
      `,
      ExpressionAttributeNames: {
        "#price": "price",
        "#name": "name",
        "#category": "category",
      },
      ExpressionAttributeValues: {
        ":price": item.price,
        ":name": item.name,
        ":category": item.category,
      },
    });

    await documentClient.send(updateCommand);
  }

  async function deleteItem(itemId: string) {
    const batchWriteCommand = new BatchWriteCommand({
      RequestItems: {
        [dynamoDBTableItemName!]: [
          {
            DeleteRequest: {
              Key: {
                itemId,
              },
            },
          },
        ],
      },
    });

    const r = await documentClient.send(batchWriteCommand);

    if (r.UnprocessedItems?.[dynamoDBTableItemName!]) {
      console.error("Unprocessed items while deleting", r.UnprocessedItems);
    }
  }
}

run().catch(console.error);
