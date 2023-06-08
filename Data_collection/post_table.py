import csv
import glob
import sqlite3

# Connect to the database
conn = sqlite3.connect("TwitterUserChanges.db")

conn.execute("CREATE TABLE IF NOT EXISTS posts (user_id text, tweet_id text, tweet_text text,"
             "tweet_likecount integer, tweet_retweetcount integer, tweet_replycount integer,"
             "tweet_mentionscount integer, tweet_urls text, tweet_location text, tweet_date text)")

conn.execute("CREATE TABLE IF NOT EXISTS hashtags (tweet_id text, tweet_hashtags text)")
conn.execute("CREATE TABLE IF NOT EXISTS terms (tweet_id text, tweet_terms text)")


# get all CSV files in the folder
csv_files = glob.glob('C:/network/StavAsafTwitt/new_post/*.csv')
# loop through each file
for file in csv_files:
    # open the CSV file
    with open(file, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)



        # Skip the first row (column names)
        column_names = next(reader)

        # Create a list of tuples from the rows in the CSV file
        rows = [row for row in reader if row and len(row) >= len(column_names)]
        i = 0
        for row in rows:
            # Check if the tweet ID already exists in the posts table
            cursor = conn.execute("SELECT tweet_id FROM posts WHERE tweet_id=?", (row[1],))
            if cursor.fetchone() is None:

            #     # If the tweet ID does not exist in the posts table, insert the row into the posts table
                conn.execute(
                    "INSERT INTO posts ( user_id, tweet_id, tweet_text, tweet_likecount, tweet_retweetcount, tweet_replycount ,tweet_mentionscount ,"
                    "tweet_urls , tweet_location ,tweet_date)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (row[0], row[1], row[2], row[3], row[4], row[6] , row[8] ,row[9] ,row[5] ,row[11]))

                row[7] = row[7].strip("[]")  # remove the brackets
                hashtags=row[7].split(",")
                for j in range (len(hashtags)):

                    #Insert the hashtags into the hashtags table
                    conn.execute("INSERT INTO hashtags (tweet_id, tweet_hashtags) VALUES (?, ?)", (row[1], hashtags[j]))

                # terms table
                conn.execute("INSERT INTO terms (tweet_id, tweet_terms) VALUES (?, ?)", (row[1], row[10]))

                # Commit the transaction
                conn.commit()
            i += 1
            print(i)
    print('next CSV')

# Close the connection
conn.close()

    



