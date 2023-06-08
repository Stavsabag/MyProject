import sqlite3
import datetime
import numpy as np

# connect to database
conn = sqlite3.connect('TwitterUserChanges.db')
conn.row_factory = sqlite3.Row

# create table if not exists
conn.execute("""
    CREATE TABLE IF NOT EXISTS processed_data (
        user_id TEXT,
        feature_name TEXT,
        value REAL,
        test_time TEXT,
        UNIQUE(user_id, feature_name)
    )
""")

# get current time
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

feature = "Exists"

# get distinct user IDs
cursor = conn.execute("SELECT DISTINCT user_id FROM users")
user_ids = [row[0] for row in cursor.fetchall()]

# get distinct user IDs
cursor = conn.execute("SELECT DISTINCT user_id FROM user_not_found")
user_ids_not_found = [row[0] for row in cursor.fetchall()]

# Define the chunk size for batch insertion
chunk_size = 19391
chunk_amount = len(user_ids)/chunk_size

# Loop through user IDs in chunks
for i in range(0, len(user_ids), chunk_size):
    user_ids_chunk = user_ids[i:i+chunk_size]
    remaining_iterations2 = chunk_size

    # Create an array to store processed data for the chunk of users
    processed_data_chunk = np.zeros((chunk_size * len(feature), 3), dtype=np.object)
    j = 0

    # Loop through each user in the chunk
    for user_id in user_ids_chunk:

        if user_id in user_ids_not_found :
            value = 1
        else:
            value = 0

        processed_data_chunk[j, 0] = user_id
        processed_data_chunk[j, 1] = "Privacy " + feature
        processed_data_chunk[j, 2] = value
        j += 1

        remaining_iterations2 -= 1
        print(("Iterations left:", remaining_iterations2))

        if j == chunk_size * len(feature) :
            break

    # Insert the chunk of data into the database
    cursor = conn.cursor()
    # cursor.executemany("INSERT OR REPLACE INTO processed_data (user_id, feature_name, value, test_time) VALUES (?, ?, ?, ?)", [(row[0], row[1], row[2], current_time) for row in processed_data_chunk if any(row)])
    values = [(row[0], row[1], row[2], current_time) for row in processed_data_chunk if any(row)]
    cursor.executemany(
        "INSERT OR REPLACE INTO processed_data (user_id, feature_name, value, test_time) VALUES (?, ?, ?, ?)", values)

    conn.commit()

    chunk_amount -= 1
    print("chunk_left:", chunk_amount)

# Close the connection to the database
conn.close()

