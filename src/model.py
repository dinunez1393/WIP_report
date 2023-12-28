# Classes
import pandas as pd
from utilities import fixed_date
from datetime import timedelta, time, datetime as dt


DAYS_BACK = 190


class UnitHistory:
    """
    Class represents an instance of a server or a rack and its checkpoints - data comes from product history
    """

    def __init__(self, checkpoints_df, sap_historicalStatus_df, today_now, isServer=True):
        """
        Constructor for class UnitHistory
        :param checkpoints_df: a dataframe containing product history data for this instance
        :type checkpoints_df: pandas.DataFrame
        :param sap_historicalStatus_df: a dataframe containing SAP historical status data for this instance
        :type sap_historicalStatus_df: pandas.DataFrame
        :param today_now: Today's date
        :type today_now: datetime.datetime
        :param isServer: A flag that indicates if the unit is a server (True) or a rack (False)
        """
        self.checkpoints_df = checkpoints_df
        self.sap_historicalStatus_df = sap_historicalStatus_df
        self.today_now = today_now
        if isServer:
            self.shipmentCkps = {300, 301, 302}
            self.criticalCkps = {100, 101, 150, 170, 202, 243, 2470, 228, 270, 230, 300, 301, 1510, 234, 302}
        else:
            self.shipmentCkps = {300, 301}
            self.criticalCkps = {200, 235, 254, 208, 252, 150, 170, 216, 218, 260, 243, 2470, 228, 270, 230, 300, 301}

    def determine_processAndArea(self):
        """
        Method determines the process and area where the server is in by iterating through each day of the server
        life cycle
        :return: A list object containing tuples, each representing a WIP instance
        :rtype: list
        """
        # Get the starter checkpoints
        mask = self.checkpoints_df['CheckPointId'].isin(self.criticalCkps)
        starterCkps_df = self.checkpoints_df[mask]
        wipHistory_tuples = []
        times = [9, 15]  # WIP Snapshot times: 9AM and 3PM

        if starterCkps_df.shape[0] > 0:
            for snapshot_time in times:
                # Find the boundaries of this unit (minimum and maximum checkpoint timestamps)
                min_timestamp = starterCkps_df['TransactionDate'].min(skipna=True)
                max_timestamp = starterCkps_df['TransactionDate'].max(skipna=True)
                today_upperBoundary = fixed_date(self.today_now, fixedHour=snapshot_time)
                starterCkps_df = starterCkps_df.sort_values('TransactionDate', ascending=False)
                # Use only for initial population of the SQL table
                minThreshold = self.today_now - timedelta(days=DAYS_BACK)

                # Get the least transaction timestamp for packing, void SN, and re-work SN
                packing_df = self.checkpoints_df[self.checkpoints_df['CheckPointId'].isin(self.shipmentCkps)]
                voidSN_df = self.checkpoints_df[self.checkpoints_df['CheckPointId'] == 700]
                rework_df = self.checkpoints_df[self.checkpoints_df['CheckPointId'] == 801]

                # Assign dummy value in the past if the current instance came from WIP table and had the flag as True
                # Assign dummy value in the future if the current instance does not have any of the flags
                # (packing, void SN, rework)
                if (packing_df['PackedPreviously_flag'].notna().any() and
                        int(packing_df['PackedPreviously_flag'].max(skipna=True))):  # Packing
                    least_packingDate = self.today_now - timedelta(days=500)
                elif packing_df.shape[0] < 1:
                    least_packingDate = self.today_now + timedelta(days=10)
                else:
                    least_packingDate = packing_df['TransactionDate'].min(skipna=True)

                if (voidSN_df['VoidSN_Previously_flag'].notna().any() and
                        int(voidSN_df['VoidSN_Previously_flag'].max(skipna=True))):  # Void SN
                    voidSN_date = self.today_now - timedelta(days=500)
                elif voidSN_df.shape[0] < 1:
                    voidSN_date = self.today_now + timedelta(days=10)
                else:
                    voidSN_date = voidSN_df['TransactionDate'].min(skipna=True)

                if (rework_df['ReworkScanPreviously_flag'].notna().any() and
                        int(rework_df['ReworkScanPreviously_flag'].max(skipna=True))):  # Re-work scan
                    rework_date = self.today_now - timedelta(days=500)
                elif rework_df.shape[0] < 1:
                    rework_date = self.today_now + timedelta(days=10)
                else:
                    rework_date = rework_df['TransactionDate'].min(skipna=True)

                # Sort SAP historical status dataframe in descending order of extraction timestamp
                if self.sap_historicalStatus_df is not None:
                    self.sap_historicalStatus_df = self.sap_historicalStatus_df.sort_values('EXTRACTED_DATE_TIME',
                                                                                            ascending=False)
                    sap_exists = True
                else:
                    sap_exists = False

                # Determine the right upper boundary
                if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
                    # Add one day to the actual upper boundary if the max timestamp is less than today's upper boundary
                    # but the time portion of max timestamp is greater than the time portion of today's upper boundary.
                    # This will allow for correct WIP counting
                    if (max_timestamp.time() > fixed_date(max_timestamp, fixedHour=snapshot_time).time() and
                            max_timestamp.date() < today_upperBoundary.date()):
                        actual_upperBoundary = fixed_date(max_timestamp, fixedHour=snapshot_time) + timedelta(days=1)
                    else:
                        actual_upperBoundary = fixed_date(max_timestamp, fixedHour=snapshot_time)
                    PackedIsLast_flag = True
                elif today_upperBoundary > max_timestamp:
                    actual_upperBoundary = today_upperBoundary
                    PackedIsLast_flag = False
                else:
                    actual_upperBoundary = fixed_date(max_timestamp, fixedHour=snapshot_time)
                    PackedIsLast_flag = False

                # Assign the WIP snapshot date to current date if unit comes from WIP table
                if starterCkps_df['WIP_SnapshotTime'].notna().any():
                    maxWip_date = starterCkps_df['WIP_SnapshotTime'].max(skipna=True).date()
                    current_date = dt.combine(maxWip_date, time(snapshot_time, 0))
                # Set usable data for very old instances  # Use only for initial population of the SQL table
                elif min_timestamp < minThreshold:
                    if starterCkps_df['CheckPointId'].iloc[0] in self.shipmentCkps:
                        if max_timestamp < minThreshold:  # Void very old instances that already shipped
                            return []
                        else:
                            current_date = fixed_date(minThreshold, fixedHour=snapshot_time)
                    else:
                        current_date = fixed_date(minThreshold, fixedHour=snapshot_time)
                else:
                    current_date = fixed_date(min_timestamp, fixedHour=snapshot_time)

                # Iterate between the boundaries to find the location (process and area) of this unit for each day
                while current_date <= actual_upperBoundary:
                    # Break from the loop if this script is running before the fixed time of the current day
                    if (self.today_now.date() == current_date.date()) and (
                            self.today_now.time() < time(snapshot_time, 0)):
                        break

                    # Get the starter checkpoints whose timestamps are less than current date
                    day_ckps_df = starterCkps_df[starterCkps_df['TransactionDate'] <= current_date]
                    if len(day_ckps_df['TransactionDate']) < 1:
                        current_date = current_date + timedelta(days=1)
                        continue
                    # Get the row that has the current location of this current instance
                    location_row = day_ckps_df.iloc[0].copy()

                    # Get the SAP status for this WIP snapshot date (current_date)
                    if sap_exists:
                        current_historicalStatus_df = \
                            self.sap_historicalStatus_df[
                                self.sap_historicalStatus_df['EXTRACTED_DATE_TIME'] <= current_date]
                        if current_historicalStatus_df.shape[0] > 0:
                            current_status = current_historicalStatus_df['STATUS'].iloc[0]
                            location_row.loc['FactoryStatus'] = current_status

                    # Timestamps fields
                    transaction_timestamp = location_row.loc['TransactionDate']
                    location_row.loc['WIP_SnapshotTime'] = current_date
                    location_row.loc['SnapshotTime'] = f"{'9AM' if snapshot_time == 9 else '3PM'}"

                    # Assign shipment status and void SN and rework flags
                    location_row.loc['PackedIsLast_flag'] = PackedIsLast_flag
                    location_row.loc['PackedPreviously_flag'] = least_packingDate < transaction_timestamp
                    location_row.loc['VoidSN_Previously_flag'] = voidSN_date < transaction_timestamp
                    location_row.loc['ReworkScanPreviously_flag'] = rework_date < transaction_timestamp

                    # Add the WIP instance to the WIP history
                    location_row_tuple = tuple(location_row)
                    wipHistory_tuples.append(location_row_tuple)
                    # Increment current day by 1
                    current_date = current_date + timedelta(days=1)

        return wipHistory_tuples
