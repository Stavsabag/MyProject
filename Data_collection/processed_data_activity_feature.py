import sqlite3
import statistics
import datetime
from scipy.stats import skew
import numpy as np

# define function to process feature changes
def process_feature_changes(rows, feature_name):
    user_changes = {}
    for row in rows:
        user_id = row['user_id']
        prev_value, curr_value = int(row['previous']), int(row['current'])
        diff = curr_value - prev_value
        user_changes.setdefault(user_id, []).append(diff)
        num_changes = len(user_changes[user_id]) if user_changes[user_id] else 0

    return {user_id: {
                "max_diff": max(user_changes[user_id], default=0),
                "min_diff": min(user_changes[user_id], default=0),
                "median_diff": statistics.median(user_changes[user_id]) if user_changes[user_id] else 0,
                "average_diff": statistics.mean(user_changes[user_id]) if user_changes[user_id] else 0,
                "std_diff": statistics.stdev(user_changes[user_id]) if num_changes > 1 else 0 if num_changes == 1 else -1,
                "var_diff": statistics.variance(user_changes[user_id]) if num_changes > 1 else 0 if num_changes == 1 else -1,
                "mode_diff": statistics.mode(user_changes[user_id]) if user_changes[user_id] else 0,
                "skew_diff": skew(user_changes[user_id], bias=False) if num_changes > 1 and statistics.stdev(user_changes[user_id]) != 0 else 100,
                "range_diff": max(user_changes[user_id], default=0) - min(user_changes[user_id], default=0),
                "mad_diff": statistics.median(
                    [abs(x - statistics.median(user_changes[user_id])) for x in user_changes[user_id]]) if user_changes[
                    user_id] else 0 if num_changes == 1 else -1,
                "cv_diff": statistics.stdev(user_changes[user_id]) / statistics.mean(user_changes[user_id]) if num_changes > 1 and statistics.mean(user_changes[user_id]) != 0 else 0 if len(user_changes[user_id]) == 1 else -1,
            }
            for user_id in user_changes}


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

# define features and new_features
features = ["followers_count", "friends_count", "listed_count" ,"statuses_count","favourites_count"]
new_features = [
"max_diff",
"min_diff",
"median_diff",
"average_diff",
"std_diff",
"var_diff",
"skew_diff",
"range_diff",
"mad_diff",
"cv_diff",
]

# get distinct user IDs
cursor = conn.execute("SELECT DISTINCT user_id FROM users")
user_ids = [row[0] for row in cursor.fetchall()]

# Define the chunk size for batch insertion
chunk_size = 19391
chunk_amount = len(user_ids)/chunk_size


# Loop through user IDs in chunks
for i in range(0, len(user_ids), chunk_size):
    user_ids_chunk = user_ids[i:i+chunk_size]
    remaining_iterations2 = chunk_size

    # Get all the rows for the user_ids_chunk for all features at once
    rows = conn.execute("SELECT user_id, previous, current, feature FROM changes WHERE user_id IN ({}) AND feature IN ({})".format(','.join('?' for _ in user_ids_chunk), ','.join('?' for _ in features)), tuple(user_ids_chunk + features)).fetchall()

    # Group the rows by user_id and feature
    grouped_rows = {}
    for row in rows:
        grouped_rows.setdefault(row['user_id'], {}).setdefault(row['feature'], []).append(row)

    # Create an array to store processed data for the chunk of users
    processed_data_chunk = np.zeros((chunk_size*len(features)*len(new_features), 3), dtype=np.object)
    j = 0

    # Loop through each user in the chunk
    for user_id in user_ids_chunk:
        # Loop through each feature for the user
        for k, feature in enumerate(features):
            # Get the rows for the user_id and feature
            rows = grouped_rows.get(user_id, {}).get(feature, [])

            # Process the feature changes and get the values for new features
            processed_feature_changes = process_feature_changes(rows, feature)

            # Insert the values into the processed data array
            for l, stat_name in enumerate(new_features):
                value = processed_feature_changes.get(user_id, {}).get(stat_name, 0 if stat_name in {"max_diff", "min_diff", "median_diff", "average_diff", "mode_diff", "range_diff", "mad_diff"} else -1 if stat_name in {"std_diff", "var_diff", "cv_diff"} else 100 if stat_name == "skew_diff" else None)
                processed_data_chunk[j, 0] = user_id
                processed_data_chunk[j, 1] = "Activity " + feature + " " + stat_name
                processed_data_chunk[j, 2] = value
                j += 1
                if j == len(new_features) :
                    break
            # if j == chunk_size*len(features)*len(new_features):
            #     break
        remaining_iterations2-=1
        print(("Iterations left:", remaining_iterations2))
        if j == chunk_size*len(features)*len(new_features):
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


