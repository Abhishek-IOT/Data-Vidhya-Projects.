import pandas as pd

# Read CSV
csv_file = "C:\\Users\\Dell\\Documents\\Data-Vidhya-Projects\\TasteTrack Analytics - Enterprise Restaurants Lakehouse\\Datasets\\Employees.csv"
df = pd.read_csv(csv_file)
# Table name
table_name = "Employee"

# Column names
columns = ", ".join(df.columns)

# Store all VALUES
values_list = []

for _, row in df.iterrows():

    row_values = []

    for value in row:

        # Handle NULL values
        if pd.isna(value):
            row_values.append("NULL")

        # Handle strings
        elif isinstance(value, str):
            value = value.replace("'", "''")
            row_values.append(f"'{value}'")

        # Handle numbers
        else:
            row_values.append(str(value))

    values_list.append("(" + ", ".join(row_values) + ")")

# Create ONE INSERT statement
insert_query = f"""
INSERT INTO {table_name}
({columns})
VALUES
{',\n'.join(values_list)};
"""

# Print query
print(insert_query)

# Save to SQL file
with open("insert_script.sql", "w", encoding="utf-8") as f:
    f.write(insert_query)

print("Single INSERT statement created successfully!")

