# Utilities functions
from datetime import datetime as dt, time, timedelta
import tkinter as tk
from tkinter import messagebox
from alerts import *
import time as ti
import logging
from tqdm import tqdm
from pathlib import Path
import os


SCRIPT_ON_SERVER = False
SRC_FOLDER = Path(__file__).parent
ERRORS_LOG = rf"{SRC_FOLDER.parent}{os.sep}Logging{os.sep}errors.log"


def fixed_date(dayDateTime, fixedHour=9):
    """
    Function returns a fixed datetime object from a datetime object. The standard fixed time is 9AM.
    :param dayDateTime: DateTime object
    :type dayDateTime: datetime.datetime
    :param fixedHour: A fixed hour during the day. Default is 9AM
    :type fixedHour: int
    :return: The combination of the date portion of dayDatetime and fixedHour
    :rtype: datetime.datetime
    """
    return dt.combine(dayDateTime.date(), time(fixedHour, 0))


def datetime_from_py_to_sql(py_datetime):
    """
    Function that converts python datetime format to SQL datetime format
    :param py_datetime: A datetime object
    :type py_datetime: datetime.datetime
    :return: A String resembling SQL datetime type
    :rtype: str
    """
    return py_datetime.__str__()[:23]


def logger_creator(logger_name, logger_type='ERROR', logFile_location=ERRORS_LOG, script_onServer=SCRIPT_ON_SERVER):
    """
    Function creates a logger object
    :param logger_name: the name assigned to the logger instance
    :type logger_name: str
    :param logger_type: the logging type (i.e. DEBUG, INFO, ERROR)
    :param logFile_location: the path location where the log file is stored
    :param script_onServer: Flag that indicates if this script is running on the server. If it is, then it should not
    save reports to log file
    :return: the logger object
    :rtype: logging.Logger
    """
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logger_type)

    # Configure handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_console_handler = logging.StreamHandler()
    logger_console_handler.setFormatter(formatter)
    logger.addHandler(logger_console_handler)

    if not script_onServer:
        logger_file_handler = logging.FileHandler(logFile_location)
        logger_file_handler.setFormatter(formatter)
        logger.addHandler(logger_file_handler)

    return logger


def show_message(alert_type, script_onServer=SCRIPT_ON_SERVER):
    """
    Function that shows an Error message box when a query does not execute properly
    Or an informational message box if the operation completed successfully
    :param alert_type: The alert type (success or error)
    :type alert_type: AlertType
    :param script_onServer: Flag. If this script is running on the server, True will turn off the alert messages
    :return: Nothing
    :rtype: None
    """
    # Do not use a message box if this script is running on the server
    if script_onServer:
        print("Nothing to see here...")
        return

    # Create a Tkinter root window
    root = tk.Tk()
    root.withdraw()

    # Show an error message box with an OK button
    if alert_type == AlertType.FAILED:
        messagebox.showerror("ERROR", "There has been an error and the operation did not complete.\n"
                                      "Please check the log for more detail.")
    # Show a message box with an OK button
    elif alert_type == AlertType.SUCCESS:
        messagebox.showinfo("UPDATE COMPLETE",
                            "Updated successfully.")

    # Close the Tkinter root window
    root.destroy()


def show_goodbye():
    """
    Function that displays a goodbye and connection close message
    :return: Nothing
    :rtype: None
    """
    # Goodbye message:
    message = "\nThe DB CONNECTION closed successfully\nGoodbye! \n"
    background = 100
    for char in message:
        print(f"\033[{background}m{char}\033[0m", end='', flush=True)
        ti.sleep(0.12)


def delta_working_hours(actual_start, actual_end, calendar=True):
    """
    Function gets the difference between two datetime objects
    Calendar flag calculates raw time delta between two datetime objects
    Standard flag (calendar=False) calculates time delta between two datetime objects excluding weekends
    :param actual_start: The actual start time
    :type actual_start: datetime.datetime
    :param actual_end: The actual end time
    :type actual_end: datetime.datetime
    :param calendar: Flag, if True calculate as calendar, if False, calculate as standard working hours
    :return: The time delta between actual_start and actual_end
    :rtype: float
    """
    seconds_between = (actual_end - actual_start).total_seconds()

    # Invalid negative value
    if seconds_between < 0:
        return 0
    elif calendar:
        return seconds_between / 3600
    else:  # Standard time calculation
        workdays_difference = actual_end.date() - actual_start.date()
        standard_start_T = time(0, 0)
        standard_end_T = time(23, 59, 59, 9999)
        weekend_days = 0

        # When the work operation occurs on the same day or up to 1 day difference, return the plain difference
        if workdays_difference < timedelta(days=2):
            return seconds_between / 3600

        # Else:
        # Assign a new start or end of actual start or actual end if date falls on the weekend
        if actual_start.weekday() == 5:
            actual_start = dt.combine(actual_start.date() + timedelta(days=2), standard_start_T)
        elif actual_start.weekday() == 6:
            actual_start = dt.combine(actual_start.date() + timedelta(days=1), standard_start_T)
        if actual_end.weekday() == 5:
            actual_end = dt.combine(actual_end.date() - timedelta(days=1), standard_end_T)
        elif actual_end.weekday() == 6:
            actual_end = dt.combine(actual_end.date() - timedelta(days=2), standard_end_T)

        # Find weekend days in between the start and end
        days_between = (actual_end - actual_start).days
        for i in range(1, days_between):
            temp_date = actual_start + timedelta(days=i)
            if temp_date.weekday() in {5, 6}:
                weekend_days += 1

        # Return the delta of two timestamps in working standard time (Monday 0:00 - Friday 23:59:59.9999)
        return ((actual_end - actual_start).total_seconds() / 3600) - (weekend_days * 24)


def str_extract_digits(str_input):
    """
    Function converts a string into an int by extracting all its digits and ignoring any non-digit character
    :param str_input: the string to convert to an int
    :return: the result of the conversion from string to integer
    :rtype: int
    """
    int_number = ''.join(filter(str.isdigit, str_input))
    return int(int_number)


def items_to_SQL_values(collection, isForUpdate=True, chunk_size=1_000):
    """
    Converts a collection of individual items into a collection of SQL values. It's useful for large UPDATE queries
    :param collection: The collection to convert to SQL values collection
    :param isForUpdate: Flag that indicates if the return values would be for the UPDATE query
    :param chunk_size: The size of the chunk for the INSERT query
    :return: The collection in SQL Values format
    :rtype: str
    """
    sql_values_str = ""
    if isForUpdate:  # Values for UPDATE query
        sql_values = ["(" + "'" + item + "'" + ")" + "," for item in collection]
        for item in tqdm(sql_values, total=len(sql_values), desc="Creating SQL Values list"):
            sql_values_str += item
        return sql_values_str[:-1]
    else:  # Placeholder values for INSERT query
        sql_values_str = "('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', {}, '{}', '{}', '{}', {}, {}, " \
                          "'{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}'),"
        sql_values_str *= chunk_size
        return sql_values_str.format(*collection)[:-1]  # Omit the last comma


def df_splitter(dataframe, category_name='SerialNumber'):
    """
    Function splits a dataframe by category into four partitions (15%-35%-20%-30%)
    :param dataframe: The dataframe to be split into four
    :type dataframe: pandas.DataFrame
    :param category_name: The column name of the category to use for the splitting
    :return: a tuple containing three dataframes, and three lists. The list contains the unique categories
    of each new DF
    :rtype: tuple
    """
    unique_categories = list(set(dataframe[category_name]))
    partition_sizes = (int(len(unique_categories) * 0.15), int(len(unique_categories) * 0.50),
                       int(len(unique_categories) * 0.70))
    division_1 = unique_categories[:partition_sizes[0]]
    division_2 = unique_categories[partition_sizes[0]: partition_sizes[1]]
    division_3 = unique_categories[partition_sizes[1]: partition_sizes[2]]
    division_4 = unique_categories[partition_sizes[2]:]
    new_df_1 = dataframe[dataframe[category_name].isin(division_1)]
    new_df_2 = dataframe[dataframe[category_name].isin(division_2)]
    new_df_3 = dataframe[dataframe[category_name].isin(division_3)]
    new_df_4 = dataframe[dataframe[category_name].isin(division_4)]

    return new_df_1, new_df_2, new_df_3, new_df_4, division_1, division_2, division_3, division_4


def unravel_df_to_chunks(dataframe, chunk_size):
    """
    Function unravels all the contents of a dataframe into a list of lists depending on the chunk size.
    :param dataframe: Dataframe containing values
    :type dataframe: pandas.DataFrame
    :param chunk_size: The size of each partition that the dataframe will be broken into
    :type chunk_size: int
    :return: A list of lists where the inner lists contain the unraveled values of the dataframe
    :rtype: list
    """
    if dataframe.shape[0] >= chunk_size:  # Partition the main dataframe into chunks of size 'chunk_size'
        num_chunks = dataframe.shape[0] // chunk_size
        chunked_dfs = [dataframe.iloc[i * chunk_size: (i + 1) * chunk_size, :].copy() for i in range(num_chunks)]
        remaining_df = dataframe.iloc[(num_chunks * chunk_size):, :].copy()

        if remaining_df.shape[0] > 0:  # Append remaining dataframe (smaller than chunk_size) if any
            chunked_dfs.append(remaining_df)

        # Unravel the partitions
        return [partition_df.values.ravel().tolist() for partition_df in chunked_dfs]

    elif 0 < dataframe.shape[0] < chunk_size:  # Small size (< chunk_size) dataframe
        return [dataframe.values.ravel().tolist()]

    return None
