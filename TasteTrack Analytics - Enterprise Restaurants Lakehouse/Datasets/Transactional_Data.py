import pandas as pd
import re

# Read the source file
df = pd.read_csv("orders.csv")

# Name of the column containing ordered items
ITEM_COLUMN = "Items in order"

expanded_rows = []

for _, row in df.iterrows():

    # Split multiple items
    items = [item.strip() for item in row[ITEM_COLUMN].split(",")]

    for item in items:

        new_row = row.copy()

        # Remove quantity like "1 x", "2 x", etc.
        cleaned_item = re.sub(r'^\d+\s*x\s*', '', item, flags=re.IGNORECASE)
        match = re.match(r'(\d+)\s*x\s*(.*)', item, re.IGNORECASE)
        quantity = int(match.group(1))

        new_row[ITEM_COLUMN] = cleaned_item.strip()
        new_row["Quantity"]=quantity
        expanded_rows.append(new_row)

# Create new dataframe
expanded_df = pd.DataFrame(expanded_rows)

# Save result
expanded_df.to_csv("orders_expanded.csv", index=False)

print(expanded_df.head())

# item='2 x Grilled Chicken Jamaican Tender, 1 x Grilled Chicken Peri Peri Tangdi'
# cleaned_item = re.sub(r'^\d+\s*x\s*', '', item, flags=re.IGNORECASE)
# match = re.match(r'(\d+)\s*x\s*(.*)', item, re.IGNORECASE)
# quantity = int(match.group(1))
# print(cleaned_item)
# print(match)
# print(quantity)