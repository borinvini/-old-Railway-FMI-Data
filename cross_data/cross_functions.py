import pandas as pd
import numpy as np
from haversine import haversine, Unit
from datetime import datetime, timedelta

def match_train_with_ems(train_stations: pd.DataFrame, ems_stations: pd.DataFrame) -> pd.DataFrame:
    """
    Matches each train station with the closest EMS station using the Haversine formula.

    Parameters:
        train_stations (pd.DataFrame): DataFrame containing train station data.
        ems_stations (pd.DataFrame): DataFrame containing EMS station data.

    Returns:
        pd.DataFrame: Train station DataFrame with additional columns for the closest EMS station,
                      its latitude, longitude, and distance in kilometers.
    """

    # Drop unnecessary columns from train stations
    train_stations = train_stations.drop(columns=["type", "stationUICCode", "countryCode", "passengerTraffic"])

    # Rename columns
    train_stations = train_stations.rename(
        columns={
            "stationName": "train_station_name",
            "stationShortCode": "train_station_short_code",
            "longitude": "train_long",
            "latitude": "train_lat",
        }
    )

    # Function to find the closest EMS station for a given train station
    def find_closest_ems(train_lat, train_lon):
        """
        Finds the closest EMS station based on Haversine distance.

        Parameters:
            train_lat (float): Latitude of the train station.
            train_lon (float): Longitude of the train station.

        Returns:
            tuple: (EMS station name, EMS latitude, EMS longitude, distance in km)
        """
        min_distance = float("inf")
        closest_ems = None
        closest_lat = None
        closest_lon = None

        for _, ems in ems_stations.iterrows():
            ems_coords = (ems["latitude"], ems["longitude"])
            train_coords = (train_lat, train_lon)

            # Compute Haversine distance
            distance = haversine(train_coords, ems_coords, unit=Unit.KILOMETERS)

            if distance < min_distance:
                min_distance = distance
                closest_ems = ems["station_name"]
                closest_lat = ems["latitude"]
                closest_lon = ems["longitude"]

        return closest_ems, closest_lat, closest_lon, min_distance

    # Apply the function to each train station
    train_stations[["closest_ems_station", "ems_latitude", "ems_longitude", "distance_km"]] = train_stations.apply(
        lambda row: find_closest_ems(row["train_lat"], row["train_long"]), axis=1, result_type="expand"
    )

    return train_stations

def merge_train_weather_data(trains_data: pd.DataFrame, fmi_data: pd.DataFrame, matched_stations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges train timetable data with the closest EMS weather observations.

    Parameters:
        trains_data (pd.DataFrame): DataFrame containing train schedule data.
        fmi_data (pd.DataFrame): DataFrame containing EMS weather observations.
        matched_stations_df (pd.DataFrame): DataFrame mapping train stations to their closest EMS stations.

    Returns:
        pd.DataFrame: Updated trains_data DataFrame with weather observations merged into timetable records.
    """

    # Extract unique departure dates
    unique_dates = trains_data["departureDate"].unique()
    print(f"🔹 Processing train and weather data for {len(unique_dates)} departure dates:")
    for date in unique_dates:
        print(f"   📅 Processing data for departure date: {date}")

    # Ensure timestamp is in datetime format (convert inplace to avoid copies)
    fmi_data["timestamp"] = pd.to_datetime(fmi_data["timestamp"], errors="coerce")

    # **Precompute EMS weather data in a dictionary for quick lookups**
    ems_weather_dict = {
        station: df.sort_values(by="timestamp").reset_index(drop=True)
        for station, df in fmi_data.groupby("station_name")
    }

    def find_closest_weather(ems_station, scheduled_time):
        """
        Finds the closest weather observation.

        Parameters:
            ems_station (str): The closest EMS station name.
            scheduled_time (str): The scheduled time in ISO format.

        Returns:
            dict: A dictionary containing the matched weather observations.
        """
        # Convert scheduled time to datetime
        scheduled_time_dt = datetime.strptime(scheduled_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        #print(f"   Scheduled Time (DT): {scheduled_time_dt}")
        
        # Ensure station exists in precomputed dictionary
        if ems_station not in ems_weather_dict:
            print(f"🚨 No weather data available for EMS '{ems_station}'")
            return {}

        # Get precomputed DataFrame for the EMS station
        station_weather_df = ems_weather_dict[ems_station]

        # **Ensure all timestamps are properly formatted as datetime**
        station_weather_df["timestamp"] = pd.to_datetime(station_weather_df["timestamp"], errors="coerce")

        # **Filter out NaT values caused by parsing errors**
        station_weather_df = station_weather_df.dropna(subset=["timestamp"])

        # Convert timestamps to a sorted numpy datetime64 array for efficient searching
        timestamps = station_weather_df["timestamp"].to_numpy(dtype="datetime64[ns]")

        # **Ensure scheduled_time_dt is also converted to numpy datetime64**
        scheduled_time_np = np.datetime64(scheduled_time_dt)
        #print(f" Scheduled Time (np): {scheduled_time_np}")

        # **Use np.searchsorted for fast timestamp lookup**
        idx = np.searchsorted(timestamps, scheduled_time_np)

        # Handle edge cases for boundary timestamps
        if idx == 0:
            closest_idx = 0
        elif idx >= len(timestamps):
            closest_idx = len(timestamps) - 1
        else:
            # Compare previous and next timestamps, choose the closest
            before = abs(timestamps[idx - 1] - scheduled_time_np)
            after = abs(timestamps[idx] - scheduled_time_np)
            closest_idx = idx if after < before else idx - 1

        # Extract weather data from the closest timestamp
        closest_row = station_weather_df.iloc[closest_idx]
        #print(f"   Closest Timestamp: {closest_row['timestamp']}")

        # **Keep the station name and rename it to closest_ems**
        weather_dict = closest_row.drop(["station_name", "timestamp"]).to_dict()

        # **Reorder the dictionary to place 'closest_ems' as the second key**
        weather_dict = {"closest_ems": closest_row["station_name"], **weather_dict}

        return weather_dict

    total_trains = len(trains_data)

    # **Use itertuples for faster iteration over DataFrame**
    for idx, train_row in enumerate(trains_data.itertuples(index=False), start=1):
        train_number = train_row.trainNumber
        departure_date = train_row.departureDate
        #print(f"🚆 Processing Train {idx}/{total_trains} - Train {train_number} on {departure_date}...")

        timetable = train_row.timeTableRows

        # Iterate over each station stop in the timetable
        for train_track in timetable:
            station_short_code = train_track.get("stationShortCode")
            scheduled_time = train_track.get("scheduledTime")
            #print(f"Scheduled Time: {scheduled_time} - Station: {station_short_code}")
            
            if station_short_code and scheduled_time:
                # Find the closest EMS station for this train station
                closest_ems_row = matched_stations_df.loc[
                    matched_stations_df["train_station_short_code"] == station_short_code
                ]

                if not closest_ems_row.empty:
                    closest_ems_station = closest_ems_row.iloc[0]["closest_ems_station"]
                    #print(f"   Closest EMS Station: {closest_ems_station}")

                    # Find the closest weather data for this EMS station and scheduled time
                    weather_data = find_closest_weather(closest_ems_station, scheduled_time)
                    #print(f"   Weather Data: {weather_data}")

                    if not weather_data:
                        print(f"   ⚠️ No weather data available for {closest_ems_station} at {scheduled_time}")

                    # Merge weather data into the stop dictionary
                    train_track["weather_observations"] = weather_data

        #print(f"✅ Train {idx}/{total_trains} - Train {train_number} processing complete!\n")

    return trains_data