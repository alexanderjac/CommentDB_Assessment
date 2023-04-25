# CommentDB_Assessment

## Solution Class ReadMe

The Solution class is a Python class that provides methods for loading data from CSV and JSON files into a MySQL database. The class takes in connection details for the MySQL database (host, user, password, and database name) as input during initialization.

## Dependencies
The following libraries are required to use the Solution class:


json
mysql.connector
os
pandas
re
datetime
pytz
Usage

## Initialization
To use the Solution class, you need to create an instance of the class by passing in the connection details for the MySQL database during initialization, like this:

### python
solution = Solution(host="localhost", user="root", password="password", database="mydb")

Make sure to replace the values for host, user, password, and database with the appropriate values for your MySQL database.

## Methods
The Solution class has the following methods:

### load_comment_text
The load_comment_text method allows you to load data from CSV files into the MySQL database. You need to pass in the path to the directory containing the CSV files as an argument, like this:

### python
solution.load_comment_text(comment_txt_path="/path/to/csv/files")

The method will create a table called "comment_text" in the MySQL database (if it doesn't already exist), and then load the data from the CSV files into this table.

## load_comment_info
The load_comment_info method allows you to load data from JSON files into the MySQL database. You need to pass in the path to the directory containing the JSON files as an argument, like this:

### python
solution.load_comment_info(folder_path="/path/to/json/files")

The method will create a table called "comment_info" in the MySQL database (if it doesn't already exist), and then load the data from the JSON files into this table.

## create_db_connection
The create_db_connection method creates a MySQL database connection and returns a mysql.connector.connection.MySQLConnection object. You can use this method to establish a connection to the MySQL database if you need to perform any other operations that are not provided by the Solution class.

## create_table
The create_table method allows you to create a table in the MySQL database. You need to pass in the table name and the column definitions as arguments. The column definitions should be provided as a list of tuples, where each tuple contains the column name and the data type, like this:

### python
columns = [("column1", "data_type1"), ("column2", "data_type2"), ...]
solution.create_table(table_name="mytable", columns=columns)

This method is used internally by the load_comment_text and load_comment_info methods to create tables in the MySQL database for loading data from CSV and JSON files.

## Note
The Solution class assumes that the CSV files have two columns named "h_id" and "message", and the JSON files have a specific structure with "h_id", "posts", and "comments" fields. If your data has a different structure, you may need to modify the code accordingly.

Also, please make sure to handle any exceptions or errors that may occur during the execution of the methods, such as invalid file paths, missing columns, or database connection errors, to ensure robustness of your application.
