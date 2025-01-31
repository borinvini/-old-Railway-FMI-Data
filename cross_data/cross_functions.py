import pandas as pd
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

import pandas as pd
from datetime import datetime

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

    # Copy trains_data to avoid modifying the original
    updated_trains_data = trains_data.copy()

    # Ensure timestamp column is in datetime format
    fmi_data["timestamp"] = pd.to_datetime(fmi_data["timestamp"])

    def find_closest_weather(ems_station, scheduled_time):
        """
        Finds the closest weather observation for a given EMS station and scheduled time.

        Parameters:
            ems_station (str): The closest EMS station name.
            scheduled_time (str): The scheduled time in ISO format.

        Returns:
            dict: A dictionary containing the matched weather observations.
        """
        # Convert scheduled time to datetime
        scheduled_time_dt = datetime.strptime(scheduled_time, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Ensure station names are stripped of leading/trailing spaces
        fmi_data["station_name"] = fmi_data["station_name"].str.strip()
        ems_station = ems_station.strip()

        # Filter FMI data by station name
        ems_weather_data = fmi_data[fmi_data["station_name"] == ems_station].copy()

        if ems_weather_data.empty:
            print(f"🚨 No weather data available for EMS '{ems_station}'")
            return {}

        # Find the closest timestamp
        ems_weather_data["time_diff"] = abs(ems_weather_data["timestamp"] - scheduled_time_dt)
        closest_row = ems_weather_data.loc[ems_weather_data["time_diff"].idxmin()]

        # Extract weather observations (drop station_name and timestamp)
        weather_dict = closest_row.drop(["station_name", "timestamp"]).to_dict()

        return weather_dict

    total_trains = len(updated_trains_data)

    # Iterate over each train in the dataset
    for idx, train_row in enumerate(updated_trains_data.itertuples(), start=1):
        train_number = train_row.trainNumber
        departure_date = train_row.departureDate
        print(f"🚆 Processing Train {idx}/{total_trains} - Train {train_number} on {departure_date}...")

        timetable = train_row.timeTableRows

        # Iterate over each station stop in the timetable
        for train_track in timetable:
            station_short_code = train_track.get("stationShortCode")
            scheduled_time = train_track.get("scheduledTime")

            if station_short_code and scheduled_time:
                # Find the closest EMS station for this train station
                closest_ems_row = matched_stations_df.loc[
                    matched_stations_df["train_station_short_code"] == station_short_code
                ]

                if not closest_ems_row.empty:
                    closest_ems_station = closest_ems_row.iloc[0]["closest_ems_station"]

                    # Find the closest weather data for this EMS station and scheduled time
                    weather_data = find_closest_weather(closest_ems_station, scheduled_time)

                    if not weather_data:
                        print(f"   ⚠️ No weather data available for {closest_ems_station} at {scheduled_time}")

                    # Merge weather data into the stop dictionary
                    train_track["weather_observations"] = weather_data

        print(f"✅ Train {idx}/{total_trains} - Train {train_number} processing complete!\n")

    return updated_trains_data
