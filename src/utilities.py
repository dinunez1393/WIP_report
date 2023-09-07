# Utilities functions
from datetime import datetime as dt, date, time, timedelta
import tkinter as tk
from tkinter import messagebox
from alerts import *
import time as ti


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


def show_message(alert_type, script_onServer=False):
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
    Function converts a string into an int by extracting all its digits and ignoring and non-digit character
    :param str_input: the string to convert to an int
    :return: the result of the conversion from string to integer
    :rtype: int
    """
    int_number = ''.join(filter(str.isdigit, str_input))
    return int(int_number)
