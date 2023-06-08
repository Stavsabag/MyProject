import tweepy
import sqlite3
from datetime import datetime
import time
import csv
import glob

# Fill in your Twitter API keys and secrets here
consumer_key = "ZZlD4QsuISg83OOp14vyxemES"
consumer_secret = "0KkZSfLz6eMVp2yxF3U8s1nSb0cwoSOZmTH753xnYsLjGS95wc"
access_token = "1598294509128933376-cQVLJ3kPmS0mC8ZmQibzhc1rPL6j6X"
access_token_secret = "K9WO14xebOL2xL3aMLdRcK9ZxvUAe8GiEY0TkyEqZZS48"

# Authenticate with the Twitter API using the keys and secrets
auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)

# Create an API object that we can use to interact with the Twitter API
api = tweepy.API(auth)

# Connect to the database
conn = sqlite3.connect("TwitterUserChanges.db")

# Create a new table with the desired structure
conn.execute("CREATE TABLE IF NOT EXISTS users (user_id TEXT,full_name TEXT NOT NULL DEFAULT '', username TEXT,"
             "followers_count INTEGER,friends_count INTEGER,listed_count INTEGER,statuses_count INTEGER ,favourites_count INTEGER , "
             "description TEXT,lang TEXT,user_location TEXT, profile_location TEXT, user_verification_status TEXT, "
             "user_profile_image_url TEXT, user_background_picture_url TEXT , profile_background_color TEXT,"
             "profile_background_tile Text,profile_link_color TEXT,profile_sidebar_border_color TEXT,"
             "profile_sidebar_fill_color TEXT,profile_text_color TEXT,profile_use_background_imgage Text, protected Text ,"
             "user_status TEXT,user_created_at DATETIME , test_time Text)")

conn.execute("CREATE TABLE IF NOT EXISTS user_not_found (user_id TEXT, error_code Text , error_message Text, test_time Text)")

status_to_test = ['alive' , 'blocked', 'not found', 'protected']

csv_files = glob.glob('C:/network/StavAsafTwitt/new_post/*.csv')

# loop through each file
for file in csv_files:
    # open the CSV file
    with open(file, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)

        # Skip the first row (column names)
        column_names = next(reader)

        # Create a list of tuples from the rows in the CSV file
        user_ids = [row for row in reader if row and len(row) >= len(column_names)]
        user_ids = [i[0] for i in user_ids]
        user_ids = list(set(user_ids))
        print(len(user_ids))


    # Get all user IDs from the post table
    # cursor = conn.execute("SELECT DISTINCT user_id FROM posts")
    # user_ids = [row[0] for row in cursor]






        j = 0
        for user_id in user_ids:
            try :
                cursor = conn.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
                if cursor.fetchone() is None:
                    user = api.get_user(user_id=user_id)
                    full_name = user.name
                    username = user.screen_name
                    followers_count = user.followers_count
                    friends_count = user.friends_count
                    listed_count = user.listed_count
                    statuses_count = user.statuses_count
                    favourites_count = user.favourites_count
                    description = user.description
                    lang = user.lang
                    user_location = user.location
                    profile_location = user.location
                    user_verification_status = "Verified" if user.verified else "Not verified"
                    user_profile_image_url = user.profile_image_url_https
                    user_background_picture_url = user.profile_background_image_url
                    profile_background_color = user.profile_background_color
                    profile_background_tile = "True" if user.profile_background_tile else "False"
                    profile_link_color = user.profile_link_color
                    profile_sidebar_border_color = user.profile_sidebar_border_color
                    profile_sidebar_fill_color = user.profile_sidebar_fill_color
                    profile_text_color = user.profile_text_color
                    profile_use_background_imgage = "True" if user.profile_use_background_image else "False"
                    protected = "protected" if user.protected else "Not protected"
                    user_status = status_to_test[0]
                    user_created_at = user.created_at
                    test_time = datetime.now()
                    test_time = test_time.strftime("%Y-%m-%d %H:%M:%S")

                    conn.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (user_id, full_name, username, followers_count, friends_count, listed_count, statuses_count,
                                favourites_count,
                                description, lang, user_location, profile_location, user_verification_status, user_profile_image_url,
                                user_background_picture_url, profile_background_color, profile_background_tile, profile_link_color,
                                profile_sidebar_border_color, profile_sidebar_fill_color, profile_text_color,
                                profile_use_background_imgage,
                                protected, user_status, user_created_at, test_time))

                    # Commit the transaction
                    conn.commit()
                j += 1
                print(j)

            except Exception as e:

                test_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if "rate limit" in str(e).lower():

                    api.wait_on_rate_limit = True

                    api.wait_on_rate_limit_notify = True
                    print(e.args[0])


                elif "tweepy.errors.TweepyException" in str(e).lower():
                    # Print an error message
                    print(f"Failed to send request: {e}")
                    # Sleep for 60 seconds before retrying
                    print(e.args[0])
                    time.sleep(60)
                    continue

                elif isinstance(e, tweepy.errors.TwitterServerError):
                    # Handle the TwitterServerError error here
                    print("There was a server error while making the request")
                    print(e.args[0])
                    continue

                 # for error 401 ,402 ,403
                elif "user not found" in str(e).lower():
                    print(
                        f"The user's id is: {user_id}. User's account not found.")
                    # error_code = '404'
                    # error_message = 'Not Found'
                    # cursor = conn.execute("SELECT user_id FROM user_not_found WHERE user_id=?", (user_id,))
                    # if cursor.fetchone() is None:
                    #     conn.execute("INSERT INTO user_not_found (user_id, error_code, error_message) VALUES (?,?,?,?)",
                    #                  (user_id, error_code, error_message,test_time))
                    #     conn.commit()
                    continue


                elif "suspended" in str(e).lower():
                    print(
                        f"The user's id is: {user_id}. The user's account is suspended.")
                    # error_code = '403'
                    # error_message = 'Forbidden'
                    # cursor = conn.execute("SELECT user_id FROM user_not_found WHERE user_id=?", (user_id,))
                    # if cursor.fetchone() is None:
                    #     conn.execute("INSERT INTO user_not_found (user_id, error_code, error_message) VALUES (?,?,?,?)",
                    #                  (user_id, error_code, error_message,test_time))
                    #     conn.commit()
                    continue


                elif isinstance(e, tweepy.errors.Unauthorized):
                    print(
                        f"The user's screen name is: {user.screen_name}. The user's account is protected and you are not following them.")
                    # error_code = '401'
                    # error_message = 'Unauthorized'
                    # cursor = conn.execute("SELECT user_id FROM user_not_found WHERE user_id=?", (user_id,))
                    # if cursor.fetchone() is None:
                    #     conn.execute("INSERT INTO user_not_found (user_id, error_code, error_message) VALUES (?,?,?,?)",
                    #                  (user_id, error_code, error_message,test_time))
                    #     conn.commit()
                    continue

    print('next CSV')

# Close the connection
conn.close()

