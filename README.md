# CommentDB_Assessment

## Solution Class ReadMe

The Solution class is a Python class that provides methods for loading data from CSV, JSON, Parquet files into a MySQL database. The class takes in connection details for the MySQL database (host, user, password, and database name) as input during initialization.


#### Prerequisites
Before running the tool, make sure you have the following software installed:

Python 3

MySQL

Required Python libraries: json, mysql.connector, os, pandas, re, datetime, pytz, logging

Setup

Clone this repository to your local machine.
Create a MySQL database and provide the database details (host, user, password, and database name) when initializing the Solution class in the etl.py file.
Usage
Import the Solution class from the etl.py file in your Python script.
Initialize an instance of the Solution class with the required database details.
Use the various methods provided by the Solution class to load data into the MySQL database.

Run the below code to set up the dependent libraries

pip install -r requirements.txt

## Methods
The Solution class provides the following methods:

create_db_connection()
This method creates a MySQL database connection and returns a mysql.connector.connection.MySQLConnection object.

create_table(table_name, columns)
This method creates a table in the MySQL database with the given table name and column definitions. The table_name parameter should be a string representing the name of the table, and the columns parameter should be a list of tuples, where each tuple contains two values: the name of the column and its data type.

load_comment_text(comment_txt_path)
This method loads data from CSV files in the specified directory into the MySQL database. The comment_txt_path parameter should be a string representing the path to the directory containing the CSV files. The data is loaded into a table named comment_text with columns h_id and message.

load_comment_info(folder_path)
This method loads data from JSON files in the specified directory into the MySQL database. The folder_path parameter should be a string representing the path to the directory containing the JSON files. The data is loaded into tables named comment_info and comment_info_comments with columns post_id, comment_id, created_time, like_count, and comment_count.

Logging
The tool logs debugging information to a file named etl.log using the logging module. You can check this log file for any errors or debugging information related to the ETL process.

Please make sure to update the logging.basicConfig method with the appropriate log level and log format as per your requirements.

#### JSON Structure

![image](https://user-images.githubusercontent.com/90269638/234447187-6521c87e-d3bd-4fc5-8543-c5a543a22c5a.png)

## How to Run the Code:

1. Install the required dependencies:

2. Set up MySQL Database: Before running the code, you need to have a MySQL database set up with the necessary credentials (host, user, password, and database name) that match the parameters used in the Solution class constructor.

3. Import the Code: Save the code to a Python file with a .py extension, and then import it into your Python script or interactive environment.

4. Create an Instance of the Solution Class: Create an instance of the Solution class by calling the constructor with the appropriate parameters for your MySQL database connection.

5. my_solution = Solution(host="your_host", user="your_user", password="your_password", database="your_database")
   Replace "your_host", "your_user", "your_password", and "your_database" with your actual MySQL database credentials or read it through a config.json file where setup the host, user, password and the database in a key_value pair.

6. Finally run the solution.py

### Implementation of each methods
Load Comment Text Data: Call the load_comment_text() method on the my_solution object, passing in the path to the directory containing the CSV files of comment text data as an argument. This method will create a table in the MySQL database (if it doesn't exist) and load the CSV data into the table.
  
my_solution.load_comment_text("path/to/comment_txt_directory")
Replace "path/to/comment_txt_directory" with the actual path to the directory containing the CSV files of comment text data.

Load Comment Info Data: Call the load_comment_info() method on the my_solution object, passing in the path to the directory containing the JSON files of comment info data as an argument. This method will create a table in the MySQL database (if it doesn't exist) and load the JSON data into the table.
 
my_solution.load_comment_info("path/to/comment_info_directory")
Replace "path/to/comment_info_directory" with the actual path to the directory containing the JSON files of comment info data.

Note: The code in the load_comment_info() method assumes that the JSON files contain data in a specific format with nested comments data. Make sure your JSON files follow this format for the code to work correctly.

Load post meta Data: Call the load_post_meta() method on the my_solution object, passing in the path to the directory containing the parquet files of comment info data as an argument. This method will create a table in the MySQL database (if it doesn't exist) and load the parquet data into the table.
 
my_solution.load_comment_info("path/to/post_meta_directory")
Replace "path/to/post_meta_directory" with the actual path to the directory containing the parquet files of post_meta data.


View Logs: The code is configured to log debug-level messages to a file named etl.log in the same directory where the code is executed. You can view the logs in this file to check for any errors or debug information related to the data loading process.

## Note
The Solution class assumes that the CSV files have two columns named "h_id" and "message", and the JSON files have a specific structure with "h_id", "posts", and "comments" fields. If your data has a different structure, you may need to modify the code accordingly.

Also, please make sure to handle any exceptions or errors that may occur during the execution of the methods, such as invalid file paths, missing columns, or database connection errors, to ensure robustness of your application.
