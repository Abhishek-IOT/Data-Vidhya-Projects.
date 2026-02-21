file_path = "Stage_Tables_DDLs.sql"


# Step 1: Read file content
with open(file_path, "r") as file:
    content = file.read()

# Step 2: Convert to uppercase
upper_content = content.upper()

# Step 3: Write back to same file
with open(file_path, "w") as file:
    file.write(upper_content)

print("File updated successfully!")