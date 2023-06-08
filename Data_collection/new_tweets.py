import csv
import snscrape.modules.twitter as sntwitter
import pandas as pd
import datetime as dt
import time
import sqlite3
import os
# Connect to the database
conn = sqlite3.connect("TwitterUserChanges.db")

# Query the new_hashtags table
query = """
    SELECT terms, hashtag, amount
    FROM new_hashtags
"""

# Load the query results into a pandas dataframe
df = pd.read_sql_query(query, conn)

# Sort the dataframe by terms and amount
df = df.sort_values(by=["terms", "amount"], ascending=[True, False])

# Get the unique terms as a set (ignoring case)
unique_terms = set(df["terms"].str.lower().unique())

# Group the dataframe by terms and select the top 2 hashtags with the largest amount for each term
term_hashtags = {}
for term, group in df.groupby("terms"):
    filtered_group = group[~group["hashtag"].str.lower().isin(unique_terms)]
    top_2_hashtags = filtered_group.head(2)[["hashtag", "amount"]]
    term_hashtags[term] = top_2_hashtags.to_dict('records')

# Extract only the hashtags from the term_hashtags dictionary and create a list
hashtags_list = [record['hashtag'] for records in term_hashtags.values() for record in records]
lowercase_hashtags_set = set(hashtag.lower() for hashtag in hashtags_list)
unique_hashtags_list = [next(hashtag for hashtag in hashtags_list if hashtag.lower() == lowercase_hashtag)
                        for lowercase_hashtag in lowercase_hashtags_set]

# Set the start and end dates for the tweet search
start_date = dt.date(2022, 1, 1)
end_date = dt.date(2023, 1, 1)

# Create the folder if it doesn't exist
folder_name = 'new_post'
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

max_tweet = 3000
remaining_iterations = len(unique_hashtags_list)

for hashtag in unique_hashtags_list:
    file_path = os.path.join(folder_name, "tweet_{}_{}_{}.csv".format(hashtag, start_date.strftime("%Y_%m_%d"),                                                           end_date.strftime("%Y_%m_%d")))
    with open(file_path, "a", encoding="utf-8") as f:
        # Create a CSV writer object with the QUOTE_MINIMAL quoting mode
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

        # Write the column names to the CSV file
        writer.writerow(
            ['tweet_user_id', 'tweet_id', 'tweet_content', 'tweet_likeCount', 'tweet_retweetCount', 'tweet_place',
             'tweet_replyCount',
             'tweet_hashtags', 'tweet_mentions', 'tweet_urls', 'terms', 'tweet_date'])
        # Iterate through the terms and their most common hashtags

        # Scrape the tweets for the hashtags
        tweets_list = []

        try:
            for tweet in sntwitter.TwitterSearchScraper(
                    f'{hashtag} since:{start_date.strftime("%Y-%m-%d")} until:{end_date.strftime("%Y-%m-%d")} lang:en').get_items():
                tweets_list.append(
                    [tweet.user.id, tweet.id, tweet.content, tweet.likeCount, tweet.retweetCount, tweet.place.fullName if tweet.place is not None else "Not available",
                        tweet.replyCount,
                        tweet.hashtags, len(tweet.mentionedUsers) if tweet.mentionedUsers is not None else 0, tweet.url, hashtag, tweet.date])

                if len(tweets_list) > max_tweet:
                    break

                else:
                    # Save the tweet to the CSV file
                    writer.writerow(
                        [tweet.user.id, tweet.id, tweet.content, tweet.likeCount, tweet.retweetCount, tweet.place.fullName if tweet.place is not None else "Not available",
                            tweet.replyCount,
                            tweet.hashtags, len(tweet.mentionedUsers) if tweet.mentionedUsers is not None else 0, tweet.url, hashtag, tweet.date])

                    print(f"{tweet.content} - @{tweet.username}")

        except Exception as e:
            print(f"Error occurred: {e}")

    remaining_iterations-=1
    print("Iterations left:", remaining_iterations)
# Close the connection
conn.close()
