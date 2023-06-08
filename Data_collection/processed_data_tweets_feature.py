import sqlite3
import datetime as dt
import snscrape.modules.twitter as sntwitter
import numpy as np


conn = sqlite3.connect('TwitterUserChanges.db')
conn.row_factory = sqlite3.Row
conn.execute("""
    CREATE TABLE IF NOT EXISTS Description_changes (
        user_id TEXT,
        Average_tweets_per_day REAL,
        num_changes REAL,
        UNIQUE(user_id, Average_tweets_per_day)
    )
""")

feature = "average tweets_per_day"


# Execute the query to get the 50 user ids with the highest value for the num_changes attribute in the Basic feature
query = """
    SELECT user_id, value
    FROM processed_data
    WHERE feature_name = 'Basic description num_changes'
    ORDER BY value DESC
    LIMIT 50
"""


cursor = conn.execute(query)

# Create a dictionary with user_id as key and value as value
user_ids_max_change_description= {row[0]: row[1] for row in cursor.fetchall()}


# Fetch all user_ids and usernames from the 'users' table
# cursor = conn.execute("SELECT user_id, username FROM users")

# Execute the query to get the usernames for the user ids in user_ids_max_change_full_name
query = f"""
    SELECT user_id, username
    FROM users
    WHERE user_id IN ({','.join(user_ids_max_change_description)})
"""
cursor = conn.execute(query)
username_dict = {row['user_id']: row['username'] for row in cursor.fetchall()}

# Update 'username_dict' for each user_id
cursor = conn.execute("SELECT user_id, current FROM changes WHERE feature='username'")
for row in cursor.fetchall():
    user_id = row['user_id']
    current_username = row['current']
    if user_id in username_dict:
        username_dict[user_id] += ',' + current_username
    else:
        continue

user_ids = list(username_dict.keys())



chunk_size = 10


# Set the start and end dates for the tweet search
start_date = dt.date(2023, 1, 10)
end_date = dt.date(2023, 4, 12)

days_diff = (end_date - start_date).days

# Create a list of all usernames for all user_ids


for i in range(0, len(user_ids), chunk_size):
    processed_data_chunk = np.zeros((chunk_size, 3), dtype=np.object)
    user_ids_chunk = user_ids[i:i+chunk_size]
    remaining_iterations2 = chunk_size
    k = 0

    for user_id in user_ids_chunk:
        count = 0
        usernames = username_dict[user_id]
        usernames = usernames.split(',')
        for username in usernames:
            for tweet in sntwitter.TwitterSearchScraper(
                    f"from:{username} since:{start_date.strftime('%Y-%m-%d')} until:{end_date.strftime('%Y-%m-%d')}").get_items():
                count += 1

        processed_data_chunk[k, 0] = user_id
        processed_data_chunk[k, 1] = count/days_diff
        processed_data_chunk[k, 2] = user_ids_max_change_description[user_id]


        k+=1

        remaining_iterations2 -= 1
        print(("Iterations left:", remaining_iterations2))

    cursor = conn.cursor()
    values = [(row[0], float(row[1]), int(row[2])) for row in processed_data_chunk if any(row)]
    cursor.executemany(
        "INSERT OR REPLACE INTO Description_changes (user_id, Average_tweets_per_day, num_changes) VALUES (?, ?, ?)", values)

    conn.commit()

conn.close()




