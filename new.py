import csv
import sqlite3
from sqlite3 import Error
import pandas as pd
import unittest

class DatabaseRecords:
    """This class contains the functions related to the Database"""
    def __init__(self):
        self.db_name = "records.db" # name of the database we're going to create and connect to
        self.file_name = "Quttinirpaaq_NP_Tundra_Plant_Phenology_2016-2017_data_1.csv" # name of the csv file containing our records
        self.csv2db()

    def csv2db(self):
        """transfers the csv records to our database"""
        # make connection with the database
        conn = sqlite3.connect(self.db_name)
        # create a Cursor object to use when executing SQL commands
        cur = conn.cursor()
        # deleting the records table if it exists (because if you run the code without deleting the existing table it will duplicate the records)
        cur.execute('''DROP TABLE IF EXISTS records''')
        # now we create a table called records that will hold our records from the csv file
        cur.execute("""CREATE TABLE IF NOT EXISTS records(Species,
                                                          Year_,
                                                          Julian_Day_of_Year,
                                                          Plant_Identification_Number,
                                                          Number_of_Buds,
                                                          Number_of_Flowers,
                                                          Number_of_Flowers_that_have_Reached_Maturity,
                                                          Observer_Initials,
                                                          Observer_Comments)""")

        # opening and reading our csv file that contains the records
        with open(self.file_name, 'r') as fin:
            # csv.DictReader uses first line in file for column headings by default
            dr = csv.DictReader(fin)  # comma is default delimiter
            to_db = [(i['Species'], i['Year'], i['Julian Day of Year'], i['Plant Identification Number'],
                      i['Number of Buds'], i['Number of Flowers'], i['Number of Flowers that have Reached Maturity'],
                      i['Observer Initials'], i['Observer Comments']) for i in dr]
        # after we read the records in our csv file we insert it in our database
        cur.executemany(
            "INSERT INTO records(Species,Year_,Julian_Day_of_Year,Plant_Identification_Number,Number_of_Buds,Number_of_Flowers,Number_of_Flowers_that_have_Reached_Maturity,Observer_Initials,Observer_Comments) VALUES (?,?,?,?,?,?,?,?,?);",
            to_db)
        conn.commit()
        # then close the connection in order to be able to use the database again.
        conn.close()

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        try:
            conn = sqlite3.connect(self.db_name)
            return conn
        except Error as e:
            print(e)

        return None

    def dataframe2db(self, dataframe):
        """Transforms Dataframe type to our Database"""
        # create database connection
        conn = sqlite3.connect(self.db_name)
        # create a Cursor object to use when executing SQL commands
        cur = conn.cursor()
        # deleting the records table if it exists (because if you run the code without deleting the existing table it will duplicate the records)
        cur.execute('''DROP TABLE IF EXISTS records''')
        # move our dataframe to our database
        dataframe.to_sql('records', conn, if_exists='replace', index=False)
        pd.read_sql('select * from records', conn)
        conn.commit()
        # close connection after using database
        conn.close()


class MainFunctions(DatabaseRecords):
    """This class contains our main functions and the inherited functions from the DatabaseRecords class"""
    def __init__(self):
        DatabaseRecords.__init__(self)
        self.dataframe = self.reload_data()
        self.is_loop_running = True

    def reload_data(self):
        """Reloads the data and returns the first 10 records"""
        try:
            file_path = "Quttinirpaaq_NP_Tundra_Plant_Phenology_2016-2017_data_1.csv"
            df = pd.read_csv(file_path, encoding="latin-1")
            df = df[1:]
            return df.head(10)
        except Exception as exception:
            return exception

    def create_new_record(self):
        """Creates a new record"""
        # dataframe containing first 10 records
        df = self.dataframe
        # creating empty dictionary
        dict = {}
        # putting the column names into a list
        columns = df.columns.tolist()
        # looping through the column names
        for col in columns:
            # inputting the value for each column name
            dict[col] = input("Enter the value for {} column: ".format(col))
        print(dict)
        # then appending the new record to our dataframe which contained 10 records
        df = df.append(dict, ignore_index=True)
        return df

    def display_record(self):
        """Displays specific records by their column value"""
        # dataframe containing first 10 records
        df = self.dataframe
        # prompts user to enter column name Ex: Species /Year /etc..
        column_name = input("Enter the column name: ")
        # prompts user to enter column value Ex: 2016
        value = input("Enter the value: ")
        return df[df[column_name] == value]

    def edit_record(self):
        """Editing records by column values"""
        # dataframe containing first 10 records
        df = self.dataframe
        # prompts user to enter column name Ex: Species /Year /etc..
        column = input("Enter the column name: ")
        # prompts user to enter column value Ex: 2016
        value = input("Enter the value: ")
        print(df.loc[df[column] == value])
        # prompts user to enter column name Ex: Species /Year /etc..
        edit_column = input("Enter the editing column: ")
        # prompts user to enter the new column value Ex: 2020
        edit_value = input("Enter the value: ")
        # assigning the new values and updating our records in the dataframe
        df.loc[df[column] == value, edit_column] = edit_value
        return df

    def delete_record(self):
        """Delete record by index"""
        # dataframe containing first 10 records
        df = self.dataframe
        # prompts a user to enter the index of the record to delete
        idx = input("Enter index of record: ")
        # dropping/ removing that record from the dataframe
        df = df.drop(df.index[int(idx)])
        return df

    def show_name(self):
        """Displays fullname of the student"""
        print("Created by: student name")

    def exit(self):
        """Abort program"""
        # assigning the is_loop_running flag to False to break/stop the loop.
        self.is_loop_running = False

    def user_interface(self):
        """Displays the options/features of the program"""
        while (self.is_loop_running):
            # Display fullname of student
            self.show_name()
            # prompts user to enter the desired option
            argument = int(input(
                "  \n 1. Read\\Reload Data                             \n 2. Add a new row                             \n 3. Display a record                             \n 4. Edit a record                             \n 5. Delete a record                             \n 6. Exit                          \n \n Enter your choice: "))
            if argument == 1:
                df = self.reload_data()
                self.dataframe2db(df)
            elif argument == 2:
                df = self.create_new_record()
                self.dataframe2db(df)
            elif argument == 3:
                df = self.display_record()
                self.dataframe2db(df)
            elif argument == 4:
                df = self.edit_record()
                self.dataframe2db(df)
            elif argument == 5:
                df = self.delete_record()
                self.dataframe2db(df)
            elif argument == 6:
                self.exit()
            else:
                print("Invalid Option !!")


class UnitTests(unittest.TestCase, MainFunctions):

    def testcase1(self):
        self.assertEqual(self.reload_data().iloc[0]['Julian Day of Year'], '169')




if __name__ == "__main__":
    m = MainFunctions()
    unittest.main()
    m.user_interface()
