import { OrderItem } from "./orderItem";

export type Order = {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  date: string;
};
