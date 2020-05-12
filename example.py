from sdbab import MariaDBClient, MongoDBClient
from pandas import DataFrame

# Set the type of the db, you should set it in a configuration file.
db_type = "mongodb"

# Create the DB client instance.
if db_type == "mariadb":
    client = MariaDBClient(
        user="neo", 
        password="10101010", 
        host="matrix.com", 
        port=1234,
        db="db_matrix",
        tbc="table_matrix"
    )
elif db_type == "mongodb":
    client = MongoDBClient(
        user="neo", 
        password="10101010", 
        host="matrix.com", 
        port=1234,
        db="db_matrix",
        tbc="collection_matrix"
    )

# Insert new data.
df = DataFrame({
    "ID1": [1, 2, 3, 4, 4], 
    "ID2": [11, 22, 33, 44, 55], 
    "A": [11.11, 22.22, 33.33, 44.44, 44.44], 
    "B": ["AAA", "BBB", "CCC", "DDD", "DDD"]
})
client.insert(df)

# Update existing data.
df_where = DataFrame({"B": ["AAA", "CCC"]})
df_new = DataFrame({"A": [111.111]})
client.update(df_where, df_new)

# Remove rows.
df_where = DataFrame({"B": ["AAA", "CCC"], "A": [111.111, 111.111]})
client.delete(df_where)

# Selecting rows.
df_where = DataFrame({"ID1": [2, 4], "A": [22.22, 444.44]})
print(client.find())
print(client.find(df_where))

# Custom user defined query function.
if db_type == "mariadb":
    def my_query(conn):
        return conn.execute("SELECT SUM(A) SUM_A FROM ctest GROUP BY ID1").fetchall()
elif db_type == "mongodb":
    def my_query(conn):
        return DataFrame(conn.aggregate([{"$group" : {"_id": "$ID1", "SUM_A": {"$sum": "$A"}}}]))
print(client.query(my_query))

# Empty the table.
client.delete()
print(client.find())