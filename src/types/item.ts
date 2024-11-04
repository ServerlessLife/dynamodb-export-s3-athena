import { ItemCategory } from "./itemCategory";

export type Item = {
  itemId: string;
  name: string;
  price: number;
  category: ItemCategory;
};
