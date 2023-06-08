import sqlite3
import pandas as pd
from collections import Counter
import re
# Connect to the database
conn = sqlite3.connect("TwitterUserChanges.db")
# Execute the query to select unique terms from the terms table
cursor = conn.execute("SELECT DISTINCT tweet_terms FROM terms")
# Fetch all rows returned by the query as a list of tuples
terms = [row[0] for row in cursor.fetchall()]
# Loop through each unique term
for term in terms:
    # Execute the query to select tweet ids for tweets containing #government
    cursor = conn.execute("SELECT tweet_id FROM terms WHERE tweet_terms=?", (term,))

    # Fetch all rows returned by the query as a list of tuples
    rows = cursor.fetchall()

    # Create a pandas DataFrame from the list of tuples
    df = pd.DataFrame(rows, columns=['tweet_id'])

    # Select only the 'tweet_id' column and convert it to a pandas array
    tweet_ids = df['tweet_id'].to_numpy()

    hashtags_list = []
    i=0
    for tweet_id in tweet_ids:
        cursor = conn.execute("SELECT tweet_hashtags FROM hashtags WHERE tweet_id=? ",
                              (tweet_id,))
        hashtags = cursor.fetchall()
        # Append the hashtags to the list
        hashtags_list.extend(hashtags)
        i+=1
        print(i)
        if i==11000:
            break
    hashtags_list = [tag[0].strip("'") for tag in hashtags_list]
    hashtags_list = [tag.strip(" '") for tag in hashtags_list]
    hashtags_list = [tag for tag in hashtags_list if tag.lower() != term[1:].lower()]
    # Flatten the list of hashtags
    hashtags_flat = [tag for tag in hashtags_list]

    # Count the frequency of each hashtag
    hashtags_count = Counter(hashtags_flat)

    # Create a pandas DataFrame of the hashtag frequencies
    hashtags_df = pd.DataFrame.from_dict(hashtags_count, orient='index', columns=['amount'])

    # Sort the DataFrame by frequency in descending order
    hashtags_df.sort_values(by='amount', ascending=False, inplace=True)

    # Add a column for the term '#government'
    hashtags_df['terms'] = term

    # Create a new table in the database and populate it with the hashtag counts
    conn.execute("CREATE TABLE IF NOT EXISTS new_hashtags (hashtag TEXT, amount INTEGER, terms TEXT)")

    for index, row in hashtags_df.iterrows():
        hashtag = "#" + index.replace(" ", "").strip("'")
        amount = row['amount']
        tweet_terms = row['terms']
        conn.execute("INSERT INTO new_hashtags (hashtag, amount, terms) VALUES (?, ?, ?)", (hashtag, amount, tweet_terms))
        conn.commit()

# Close the connection
conn.close()







