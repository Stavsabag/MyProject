import csv
import snscrape.modules.twitter as sntwitter
import pandas as pd
import datetime as dt
import time

# Read the hashtags from the hashtags file
hashtags = []
with open("Specific_hashtag.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        hashtags.append(row[0])

# Set the start and end dates for the tweet search
start_date = dt.date(2022, 1, 1)
end_date = dt.date(2023, 1, 1)

with open("tweet_{}-{}_{}.csv".format(hashtags, start_date.strftime("%Y_%m_%d"), end_date.strftime("%Y_%m_%d")), "a", encoding="utf-8") as f:
    # Create a CSV writer object with the QUOTE_MINIMAL quoting mode
    writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

    # Write the column names to the CSV file
    writer.writerow(
        ['tweet_user_id', 'tweet_id', 'tweet_content', 'tweet_likeCount', 'tweet_retweetCount', 'tweet_place',
         'tweet_replyCount',
         'tweet_hashtags', 'tweet_mentions', 'tweet_urls', 'terms', 'tweet_date'])
    max_tweet = 10000
    # Iterate through the hashtags
    for hashtag in hashtags:
        # Scrape the tweets for the hashtag
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
















