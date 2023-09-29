# Classes
import pandas as pd
from utilities import fixed_date, delta_working_hours
from datetime import datetime as dt, date, time, timedelta


DAYS_BACK = 120


class ServerHistory:
    """
    Class represents an instance of a server and its checkpoints - data comes from product history
    """

    def __init__(self, sr_checkpoints_df, sap_historicalStatus_df):
        self.sr_checkpoints_df = sr_checkpoints_df
        self.sap_historicalStatus_df = sap_historicalStatus_df
        self.shipmentCkps = {300, 301, 302}
        self.criticalCkps = {100, 101, 200, 235, 254, 208, 252, 150, 170, 216, 218, 260, 243, 2470, 228, 270, 230, 300,
                             301, 1510, 234, 302}
        self.areas = {'Server Build': [100, 101],
                      'Rack Build': [200, 235, 254, 208, 252],
                      'System Test': [150, 170],
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

        if starterCkps_df.shape[0] > 0:
            # Find the boundaries of this server (minimum and maximum checkpoint timestamps)
            min_timestamp = starterCkps_df['TransactionDate'].min(skipna=True)
            max_timestamp = starterCkps_df['TransactionDate'].max(skipna=True)
            today_upperBoundary = fixed_date(dt.now())
            starterCkps_df = starterCkps_df.sort_values('TransactionDate', ascending=False)
            minThreshold = dt.now() - timedelta(days=DAYS_BACK)  # Use only for initial population of the SQL table

            # Get the least packing transaction timestamp
            packing_df = self.sr_checkpoints_df[self.sr_checkpoints_df['CheckPointId'].isin(self.shipmentCkps)]
            # Assign dummy value in the future if the current instance does not have a packing date yet
            if packing_df.shape[0] < 1:
                least_packingDate = dt.now() + timedelta(days=10)
            else:
                least_packingDate = packing_df['TransactionDate'].min(skipna=True)

            # Sort SAP historical status dataframe in descending order of extraction timestamp
            if self.sap_historicalStatus_df is not None:
                self.sap_historicalStatus_df = self.sap_historicalStatus_df.sort_values('EXTRACTED_DATE_TIME',
                                                                                        ascending=False)

            # Determine the right upper boundary
            if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
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

            # Disabled indefinitely
            # # Find the lower boundary. Take the actual minimum timestamp if it is a new instance. Else, add one day
            # # to the minium timestamp for existing WIP
            # if 'isFrom_WIP' in set(starterCkps_df['isFrom_WIP']):
            #     previousData_df = starterCkps_df[starterCkps_df['TransactionDate'] <= min_timestamp]
            #     current_date = previousData_df['SnapshotTime'].max(skipna=True) + timedelta(days=1)
            # else:
            #     current_date = fixed_date(min_timestamp)

            current_date = fixed_date(min_timestamp)
            # Set usable data for very old instances  # Use only for initial population of the SQL table
            if min_timestamp < minThreshold:
                if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
                    if max_timestamp < minThreshold:  # Void very old instances that already shipped
                        return []
                    else:
                        current_date = fixed_date(minThreshold)
                else:
                    current_date = fixed_date(minThreshold)

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
                # Get the SAP status for this WIP snapshot date (current_date)
                if self.sap_historicalStatus_df is not None:
                    current_historicalStatus_df = \
                        self.sap_historicalStatus_df[
                            self.sap_historicalStatus_df['EXTRACTED_DATE_TIME'] <= current_date]
                    if current_historicalStatus_df.shape[0] > 0:
                        current_status = current_historicalStatus_df['STATUS'].iloc[0]
                        location_row['FactoryStatus'] = current_status
                # Calculate Working dwell time
                transaction_timestamp = location_row['TransactionDate']
                location_row['SnapshotTime'] = current_date
                location_row['DwellTime_working'] = delta_working_hours(transaction_timestamp, current_date,
                                                                        calendar=False)
                # Assign shipment status
                location_row['NotShippedTransaction_flag'] = notShippedTransaction_flag
                location_row['PackedPreviously_flag'] = least_packingDate < transaction_timestamp
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

    def __init__(self, re_checkpoints_df, sap_historicalStatus_df):
        self.re_checkpoints_df = re_checkpoints_df
        self.sap_historicalStatus_df = sap_historicalStatus_df
        self.shipmentCkps = {300, 301}
        self.criticalCkps = {200, 235, 254, 208, 252, 150, 170, 216, 218, 260, 243, 2470, 228, 270, 230, 300, 301}
        self.areas = {'Rack Build': [200, 235, 254, 208, 252],
                      'System Test': [150, 170],
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

        if starterCkps_df.shape[0] > 0:
            # Find the boundaries of this rack (minimum and maximum checkpoint timestamps)
            min_timestamp = starterCkps_df['TransactionDate'].min(skipna=True)
            max_timestamp = starterCkps_df['TransactionDate'].max(skipna=True)
            today_upperBoundary = fixed_date(dt.now())
            starterCkps_df = starterCkps_df.sort_values('TransactionDate', ascending=False)
            minThreshold = dt.now() - timedelta(days=DAYS_BACK)  # Use only for initial population of the SQL table

            # Get the least packing transaction timestamp
            packing_df = self.re_checkpoints_df[self.re_checkpoints_df['CheckPointId'].isin(self.shipmentCkps)]
            # Assign dummy value in the future if the current instance does not have a packing date yet
            if packing_df.shape[0] < 1:
                least_packingDate = dt.now() + timedelta(days=10)
            else:
                least_packingDate = packing_df['TransactionDate'].min(skipna=True)

            # Sort SAP historical status dataframe in descending order of extraction timestamp
            if self.sap_historicalStatus_df is not None:
                self.sap_historicalStatus_df = self.sap_historicalStatus_df.sort_values('EXTRACTED_DATE_TIME',
                                                                                        ascending=False)

            # Determine the right upper boundary
            if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
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

            # Disabled indefinitely
            # # Find the lower boundary. Take the actual minimum timestamp if it is a new instance. Else, add one day
            # # to the minium timestamp for existing WIP
            # if 'isFrom_WIP' in set(starterCkps_df['isFrom_WIP']):
            #     previousData_df = starterCkps_df[starterCkps_df['TransactionDate'] <= min_timestamp]
            #     current_date = previousData_df['SnapshotTime'].max(skipna=True) + timedelta(days=1)
            # else:
            #     current_date = fixed_date(min_timestamp)

            current_date = fixed_date(min_timestamp)
            # Set usable data for very old instances  # Use only for initial population of the SQL table
            if min_timestamp < minThreshold:
                if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
                    if max_timestamp < minThreshold:  # Void very old instances that already shipped
                        return []
                    else:
                        current_date = fixed_date(minThreshold)
                else:
                    current_date = fixed_date(minThreshold)

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
                # Get the SAP status for this WIP snapshot date (current_date)
                if self.sap_historicalStatus_df is not None:
                    current_historicalStatus_df = \
                        self.sap_historicalStatus_df[
                            self.sap_historicalStatus_df['EXTRACTED_DATE_TIME'] <= current_date]
                    if current_historicalStatus_df.shape[0] > 0:
                        current_status = current_historicalStatus_df['STATUS'].iloc[0]
                        location_row['FactoryStatus'] = current_status
                # Calculate Working dwell time
                transaction_timestamp = location_row['TransactionDate']
                location_row['SnapshotTime'] = current_date
                location_row['DwellTime_working'] = delta_working_hours(transaction_timestamp, current_date,
                                                                        calendar=False)
                # Assign shipment status
                location_row['NotShippedTransaction_flag'] = notShippedTransaction_flag
                location_row['PackedPreviously_flag'] = least_packingDate < transaction_timestamp
                # Add the WIP instance to the WIP history
                location_row_tuple = tuple(location_row)
                wipHistory_tuples.append(location_row_tuple)
                # Increment current day by 1
                current_date = current_date + timedelta(days=1)

        return wipHistory_tuples
