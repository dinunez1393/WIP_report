# Classes
import pandas as pd
import numpy as np
from utilities import fixed_date
from datetime import datetime as dt, date, time, timedelta
import re


class ServerHistory:
    """
    Class represents an instance of a server and its checkpoints - data comes from product history
    """

    def __init__(self, sr_checkpoints_df):
        self.sr_checkpoints_df = sr_checkpoints_df
        self.shipmentCkps = {300, 301, 302}
        self.criticalCkps = {100, 200, 235, 254, 208, 252, 150, 216, 218, 260, 243, 2470, 228, 270, 230, 300, 301, 1510,
                             234, 302}
        self.areas = {'Server Build': [100],
                      'Rack Build': [200, 235, 254, 208, 252],
                      'System Test': [150],
                      'End of Line': [216, 218, 260, 202, 243, 2470, 228, 270, 237, 230, 300, 301, 1510, 234, 302]}

    def determine_processAndArea(self):
        """
        Method determines the process and area where the server is in by iterating through each day of the server
        life cycle
        :return: A list object containing tuples, each representing a WIP instance
        :rtype: list
        """
        # Get the starter checkpoints
        mask = self.sr_checkpoints_df['CheckPointId'].isin(self.criticalCkps)
        starterCkps_df = self.sr_checkpoints_df[mask]
        wipHistory_tuples = []

        # Find the boundaries of this server (minimum and maximum checkpoint timestamps)
        min_timestamp = self.sr_checkpoints_df['TransactionDate'].min(skipna=True)
        max_timestamp = self.sr_checkpoints_df['TransactionDate'].max(skipna=True)
        today_upperBoundary = fixed_date(dt.now())
        self.sr_checkpoints_df = self.sr_checkpoints_df.sort_values('TransactionDate', ascending=False)
        if self.sr_checkpoints_df['CheckPointId'].iloc[0] in self.shipmentCkps:
            # Add one day to the actual upper boundary if the max timestamp is less than today's upper boundary
            # but the time portion of max timestamp is greater than the time portion of today's upper boundary. This
            # will allow for correct WIP counting
            if (max_timestamp.time() > fixed_date(max_timestamp).time() and
                    max_timestamp.date() < today_upperBoundary.date()):
                actual_upperBoundary = fixed_date(max_timestamp) + timedelta(days=1)
            else:
                actual_upperBoundary = fixed_date(max_timestamp)
            notShippedTransaction_flag = False
        elif today_upperBoundary > max_timestamp:
            actual_upperBoundary = today_upperBoundary
            notShippedTransaction_flag = True
        else:
            actual_upperBoundary = fixed_date(max_timestamp)
            notShippedTransaction_flag = True

        # Find the lower boundary. Take the actual minimum timestamp if it is a new instance. Else, add one day
        # to the minium timestamp for existing WIP
        if 7131993 in starterCkps_df['isFrom_WIP']:
            current_date = fixed_date(min_timestamp) + timedelta(days=1)
        else:
            current_date = fixed_date(min_timestamp)

        # Iterate between the boundaries to find the location (process and area) of this server for each day
        while current_date <= actual_upperBoundary:
            # Get the starter checkpoints whose timestamps are less than current date
            day_ckps_df = starterCkps_df[starterCkps_df['TransactionDate'] <= current_date]
            if len(day_ckps_df['TransactionDate']) < 1:
                current_date = current_date + timedelta(days=1)
                continue
            # Get the row that has the current location of the server
            location_row = day_ckps_df[day_ckps_df['TransactionDate'] ==
                                       day_ckps_df['TransactionDate'].max(skipna=True)].iloc[0]
            # Add the areas where the server is located for this date and also add the fixed current date as columns
            for area, checkpoint_id in self.areas.items():
                if location_row['CheckPointId'] in checkpoint_id:
                    location_row['Area'] = area
                    break
            location_row['SnapshotTime'] = current_date
            # Assign shipment status
            location_row['NotShippedTransaction_flag'] = notShippedTransaction_flag
            # Add the WIP instance to the WIP history
            location_row_tuple = tuple(location_row)
            wipHistory_tuples.append(location_row_tuple)
            # Increment current day by 1
            current_date = current_date + timedelta(days=1)

        return wipHistory_tuples


class RackHistory:
    """
    Class represents an instance of a rack and its checkpoints - data comes from product history
    """

    def __init__(self, re_checkpoints_df):
        self.re_checkpoints_df = re_checkpoints_df
        self.shipmentCkps = {300, 301}
        self.criticalCkps = {200, 235, 254, 208, 252, 150, 216, 218, 260, 243, 2470, 228, 270, 230, 300, 301}
        self.areas = {'Rack Build': [200, 235, 254, 208, 252],
                      'System Test': [150],
                      'End of Line': [216, 218, 260, 202, 243, 2470, 228, 270, 237, 230, 300, 301]}

    def determine_processAndArea(self):
        """
        Method determines the process and area where the rack is in by iterating through each day of the server
        life cycle
        :return: A list object containing tuples, each representing a WIP instance
        :rtype: list
        """
        # Get the starter checkpoints
        mask = self.re_checkpoints_df['CheckPointId'].isin(self.criticalCkps)
        starterCkps_df = self.re_checkpoints_df[mask]
        wipHistory_tuples = []

        # Find the boundaries of this rack (minimum and maximum checkpoint timestamps)
        min_timestamp = self.re_checkpoints_df['TransactionDate'].min(skipna=True)
        max_timestamp = self.re_checkpoints_df['TransactionDate'].max(skipna=True)
        today_upperBoundary = fixed_date(dt.now())
        self.re_checkpoints_df = self.re_checkpoints_df.sort_values('TransactionDate', ascending=False)
        if self.re_checkpoints_df['CheckPointId'].iloc[0] in self.shipmentCkps:
            # Add one day to the actual upper boundary if the max timestamp is less than today's upper boundary
            # but the time portion of max timestamp is greater than the time portion of today's upper boundary. This
            # will allow for correct WIP counting
            if (max_timestamp.time() > fixed_date(max_timestamp).time() and
                    max_timestamp.date() < today_upperBoundary.date()):
                actual_upperBoundary = fixed_date(max_timestamp) + timedelta(days=1)
            else:
                actual_upperBoundary = fixed_date(max_timestamp)
            notShippedTransaction_flag = False
        elif today_upperBoundary > max_timestamp:
            actual_upperBoundary = today_upperBoundary
            notShippedTransaction_flag = True
        else:
            actual_upperBoundary = fixed_date(max_timestamp)
            notShippedTransaction_flag = True

        # Find the lower boundary. Take the actual minimum timestamp if it is a new instance. Else, add one day
        # to the minium timestamp for existing WIP
        if 7131993 in starterCkps_df['isFrom_WIP']:
            current_date = fixed_date(min_timestamp) + timedelta(days=1)
        else:
            current_date = fixed_date(min_timestamp)

        # Iterate between the boundaries to find the location (process and area) of this rack for each day
        while current_date <= actual_upperBoundary:
            # Get the starter checkpoints whose timestamps are less than current date
            day_ckps_df = starterCkps_df[starterCkps_df['TransactionDate'] <= current_date]
            if len(day_ckps_df['TransactionDate']) < 1:
                current_date = current_date + timedelta(days=1)
                continue
            # Get the row that has the current location of the rack
            location_row = day_ckps_df[day_ckps_df['TransactionDate'] ==
                                       day_ckps_df['TransactionDate'].max(skipna=True)].iloc[0]
            # Add the areas where the rack is located for this date and also add the fixed current date as columns
            for area, checkpoint_id in self.areas.items():
                if location_row['CheckPointId'] in checkpoint_id:
                    location_row['Area'] = area
                    break
            location_row['SnapshotTime'] = current_date
            # Assign shipment status
            location_row['NotShippedTransaction_flag'] = notShippedTransaction_flag
            # Add the WIP instance to the WIP history
            location_row_tuple = tuple(location_row)
            wipHistory_tuples.append(location_row_tuple)
            # Increment current day by 1
            current_date = current_date + timedelta(days=1)

        return wipHistory_tuples
