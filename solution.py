import json
import mysql.connector
import os
import pandas as pd
import re
from datetime import datetime
import pytz
import logging

logging.basicConfig(filename='etl.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Solution:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_db_connection(self):
        """
        Create a MySQL database connection.

        :return: mysql.connector.connection.MySQLConnection object
        """
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return conn
    def create_table(self, table_name, columns):
        """
        Create a table in the MySQL database.

        :param table_name: str, name of the table
        :param columns: list of tuples, each tuple containing column name and data type
        """
        # Connect to database
        conn = self.create_db_connection()
        cursor = conn.cursor()
        column_defs = []
        
        for col in columns:
            column_name, data_type = col[0], col[1]
            column_defs.append(f"{column_name} {data_type}")
        column_defs_str = ", ".join(column_defs)

      # Execute the SQL statement to create the table
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs_str})")
        # Commit and close the database connection

        conn.commit()
        cursor.close()
        conn.close()


    def load_comment_text(self, comment_txt_path):
        """
        Load CSV files from a directory into MySQL database.

        :param comment_txt_path: str, path to the directory containing CSV files
        """

        # Define column names and data types for the comments table
        columns = [("h_id", "LONGTEXT"), ("message", "LONGTEXT")]
        table_name = "comment_text"

        # Create the comments table
        self.create_table(table_name, columns)

        # Connect to the database and create a cursor
        conn = self.create_db_connection()
        cursor = conn.cursor()

        # Iterate through each file in the directory
        for file in os.listdir(comment_txt_path):
            if file.endswith(".csv"):
                file_path = os.path.join(comment_txt_path, file)

                # Read CSV file into a pandas DataFrame
                df = pd.read_csv(file_path,encoding='utf-8')

                # Load DataFrame into MySQL
                df.apply(lambda row: cursor.execute("INSERT INTO {} (h_id, message) VALUES (%s, %s)".format(table_name), (row['h_id'], row['message'])), axis=1)

        # Commit the transaction and close the cursor and database connection
        conn.commit()
        cursor.close()
        conn.close()
    

        

    def load_comment_info(self, folder_path):
        """
        Load JSON files from a directory into MySQL database.

        :param folder_path: str, path to the directory containing JSON files
        """
    # Create an empty DataFrame to store the normalized data
        df_combined = pd.DataFrame()

        def load_comments(post_data, comments_data, df):
            """
            Recursively load comments data and append to DataFrame.

            :param post_data: dict, data of the parent post
            :param comments_data: dict, comments data
            :param df: pd.DataFrame, current combined DataFrame
            :return: pd.DataFrame, updated combined DataFrame
            """

            # Normalize comments data and append to combined DataFrame
            if "comments" in comments_data:
                comments = comments_data["comments"]["data"]
                for comment in comments:
                    comment_data = {"post_id": post_data["post_id"],
                                    "comment_id": comment["h_id"],
                                    "created_time": comment.get("created_time", datetime(1970, 1, 16, 0, 51, 52, tzinfo=pytz.utc)),
                                    "like_count": max(comment.get("up_likes", 0), comment.get("like_count", 0)),
                                    "comment_count": comment.get("comment_count", 0)}
                    df = pd.concat([df, pd.json_normalize(comment_data)], ignore_index=True)
                    df = load_comments(post_data, comment, df)
            return df

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".json"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r') as file:
                    for line in file:
                        try:
                            data = json.loads(line)
                            # Extract relevant fields from the JSON object
                            post_id = data["h_id"]
                            if "posts" in data:
                                posts = data["posts"]["data"]
                                for post in posts:
                                    post_data = {"post_id": post["h_id"],
                                                "created_time": post.get("created_time", datetime(1970, 1, 16, 0, 51, 52, tzinfo=pytz.utc)),
                                                "like_count": max(post.get("up_likes", 0), post.get("like_count", 0)),
                                                "comment_count": post.get("comment_count", 0)}
                                    df_combined = pd.concat([df_combined, pd.json_normalize(post_data)], ignore_index=True)
                                    df_combined = load_comments(post_data, post, df_combined)
                            else:
                                df_combined = pd.concat([df_combined, pd.json_normalize({"post_id": post_id,
                                                                                        "created_time": datetime(1970, 1, 16, 0, 51, 52, tzinfo=pytz.utc),
                                                                                        "like_count": 0,
                                                                                        "comment_count": 0})], ignore_index=True)
                        except json.JSONDecodeError:
                            print(f"Error parsing JSON in file: {file_path}")
                            logging.error(f"Error parsing JSON in file: {file_path}", f"An error occurred while loading comment text: {json.JSONDecodeError}")
                            continue

        # Drop duplicates based on the "post_id", "created_time", "like_count", "comment_count" columns, keeping the row with highest values
        df_combined = df_combined.sort_values(by=["created_time", "like_count", "comment_count"], ascending=False)
        df_combined.drop_duplicates(subset=["post_id", "created_time", "like_count", "comment_count"], keep="first", inplace=True)

        # return df_combined
        df_combined['created_time'] = pd.to_datetime(df_combined['created_time'])  # Convert created_time column to datetime data type
        df_combined['created_time'] = df_combined['created_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) # Convert created_time to string representation of timestamp
        columns = [("post_id", "LONGTEXT"), ("comment_id", "LONGTEXT"),("created_time", "DATETIME"),("like_count", "INT"),("comment_count", "INT")]
        table_name = "comment_info"

        # Create the table in the database
        self.create_table(table_name, columns)  
        conn = self.create_db_connection()
        cursor = conn.cursor()

        # Insert data into the database table
        df_combined.apply(lambda row: cursor.execute("INSERT INTO {} (post_id, comment_id, created_time, like_count, comment_count) VALUES (%s, %s, %s, %s, %s)".format(table_name), (row['post_id'], row['comment_id'], row['created_time'], row['like_count'], row['comment_count'])), axis=1)
        # Commit the changes to the database and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()

    def load_post_meta(self, folder_path):
        """
        Loads post meta data from Parquet files in a folder into MySQL.

        :param folder_path (str): The folder path containing the Parquet files.
        """
        
        columns = [ ("created_time", "DATETIME"), ("type", "VARCHAR(5)"), ("post_h_id", "LONGTEXT"),("page_h_id", "LONGTEXT"),("name_h_id", "LONGTEXT")]
        table_name = "post_meta"
        self.create_table(table_name, columns)
        files = os.listdir(folder_path)
        parquet_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.parquet')]
        merged_df = pd.DataFrame()
        for file in parquet_files:
                # Create the file path by joining the folder path and file name
                file_path = os.path.join(folder_path, file)

                # Read the Parquet file into a temporary dataframe
                temp_df = pd.read_parquet(file_path)

                # Concatenate the temporary dataframe with the merged dataframe
                merged_df = pd.concat([merged_df, temp_df], ignore_index=True)
        merged_df['created_time'] = pd.to_datetime(merged_df['created_time'])  # Convert created_time column to datetime data type
        conn = self.create_db_connection()
        cursor = conn.cursor()

        for index, row in merged_df.iterrows():
            created_time = row["created_time"].strftime('%Y-%m-%d %H:%M:%S')  # Convert timestamp to string format
            cursor.execute(f"INSERT INTO {table_name} (created_time, type, post_h_id, page_h_id, name_h_id) "
                        f"VALUES (%s, %s, %s, %s, %s)",
                        (created_time, row["type"], row["post_h_id"], row["page_h_id"], row["name_h_id"]))
        # Commit the changes to the database and close the cursor and connection

        conn.commit()
        cursor.close()
        conn.close()

# Entry point of the script
if __name__ == "__main__":
    # Load configuration from config.json file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    # Extract relevant configuration values
    host = config['host']
    user = config['user']
    password = config['password']
    database = config['database']
    comment_txt_path = config['comment_txt_path']
    comment_info_path = config['comment_info_path']
    post_meta_path = config['post_meta_path']
    # Create an instance of the Solution class with the extracted configuration values
    sol = Solution(host, user, password, database)

    # Load comment text data into MySQL
    sol.load_comment_text(comment_txt_path)
    # Load comment info data into MySQL
    sol.load_comment_info(comment_info_path)
    # Load post meta data into MySQL
    sol.load_post_meta(post_meta_path)
