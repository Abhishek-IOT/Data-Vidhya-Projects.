from databricks import sql

connection = sql.connect(
    server_hostname="",
    http_path="",
    access_token=""
)

cursor = connection.cursor()
cursor.execute("SELECT current_catalog(), current_schema()")
print(cursor.fetchall())