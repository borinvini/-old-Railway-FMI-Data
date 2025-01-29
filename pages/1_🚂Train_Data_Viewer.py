import pandas as pd
import os
import streamlit as st
import folium
from streamlit_folium import st_folium

from misc.const import CATEGORY, CSV_ALL_TRAINS, FOLDER_NAME, OPERATOR, START_DATE, CSV_TRAIN_STATIONS

# Set the page configuration
st.set_page_config(
    page_title="Trans Data Viewer",  # The title displayed in the browser tab
    page_icon="🚂",                   # Optional: icon for the tab and sidebar
    layout="wide",                    # Optional: layout mode (wide or centered)
    initial_sidebar_state="expanded"  # Optional: expand or collapse sidebar
)

# Title of the Streamlit App
st.title("Train Data Viewer")

# Path to the CSV file
csv_file = os.path.join(FOLDER_NAME, CSV_ALL_TRAINS)  # Adjust path if necessary
train_stations_file = os.path.join(FOLDER_NAME, CSV_TRAIN_STATIONS)

# Session State to Manage Loading and Delay
if "show_table" not in st.session_state:
    st.session_state.show_table = False

# If the table is not ready to be shown
if not st.session_state.show_table:
    # Inform the user that the CSV file is being loaded
    st.info("Loading train data from the CSV file. Please wait...")

    try:
        # Load the DataFrame from the CSV file
        train_data = pd.read_csv(csv_file)

        # Message indicating that the loading is complete
        st.success("Train data successfully loaded from the CSV file! 🚂")

        # Update session state to show the table and rerun the app
        st.session_state.show_table = True
        st.rerun()

    except FileNotFoundError:
        st.error(f"The CSV file '{csv_file}' was not found.")
    except Exception as e:
        st.error(f"An error occurred while loading the CSV file: {e}")

# If the table is ready to be shown
else:
    try:
        # Load the DataFrame from the CSV file
        train_data = pd.read_csv(csv_file)

        # Check if the DataFrame is not empty
        if not train_data.empty:
            # Filter by departureDate with default value
            unique_dates = train_data["departureDate"].sort_values().unique()
            selected_date = st.selectbox(
                "Select Departure Date",
                options=unique_dates,
                index=list(unique_dates).index(START_DATE) if START_DATE in unique_dates else 0,
            )

            # Create two columns for trainCategory and operatorShortCode
            col1, col2 = st.columns(2)

            # Filter by trainCategory with an "All Categories" option
            with col1:
                unique_categories = ["All Categories"] + list(train_data["trainCategory"].sort_values().unique())
                selected_category = st.selectbox(
                    "Select Train Category",
                    options=unique_categories,
                    index=unique_categories.index(CATEGORY) if CATEGORY in unique_categories else 0,
                )

            # Filter by operatorShortCode with an "All Operators" option
            with col2:
                unique_operators = ["All Operators"] + list(train_data["operatorShortCode"].sort_values().unique())
                selected_operator = st.selectbox(
                    "Select Operator Short Code",
                    options=unique_operators,
                    index=unique_operators.index(OPERATOR) if OPERATOR in unique_operators else 0,
                )

            # Filter by cancelled status
            show_cancelled = st.checkbox("Show Only Cancelled Trains", value=False)

            # Apply filters to the DataFrame
            filtered_data = train_data[
                (train_data["departureDate"] == selected_date) &
                ((train_data["trainCategory"] == selected_category) if selected_category != "All Categories" else True) &
                ((train_data["operatorShortCode"] == selected_operator) if selected_operator != "All Operators" else True) &
                ((train_data["cancelled"] == True) if show_cancelled else (train_data["cancelled"] == train_data["cancelled"]))
            ]

            # Display a small message for the data being shown
            st.caption("Displaying filtered train data for the selected criteria.")

            # Display the filtered DataFrame
            st.dataframe(filtered_data, height=800, width=1000)

            # Additional information about the DataFrame
            st.write("Total number of rows:", filtered_data.shape[0])
            st.write("Number of columns:", filtered_data.shape[1])

            # Select a specific trainNumber from the filtered data
            if not filtered_data.empty:
                st.subheader("Select a Train Number")

                # Get the unique train numbers for the filtered date
                unique_train_numbers = filtered_data["trainNumber"].sort_values().unique()

                # Create a selectbox for train numbers
                selected_train_number = st.selectbox(
                    "Select Train Number",
                    options=unique_train_numbers,
                    index=0
                )

                # Display legend
                st.subheader("Marker Legend")
                st.markdown(
                    """
                    - <span style="color:blue;">**Blue**</span>: Train is on time  
                    - <span style="color:red;">**Red**</span>: Train is delayed  
                    - <span style="color:green;">**Green**</span>: Train is ahead of the schedule  
                    """,
                    unsafe_allow_html=True  # Allows HTML rendering
                )


                # Display the timetable rows for the selected train
                st.subheader("Train Route Map")
                train_timetable = filtered_data.loc[
                    filtered_data["trainNumber"] == selected_train_number, "timeTableRows"
                ].values[0]

                # Convert the timetable data to a DataFrame
                timetable_df = pd.DataFrame(eval(train_timetable))

                # Define the preferred column order
                preferred_columns = [
                    "stationName",
                    "stationShortCode",
                    "type",
                    "scheduledTime",
                    "actualTime",
                    "differenceInMinutes",
                    "cancelled",
                ]

                # Reorder the columns: preferred columns first, then the rest
                reordered_columns = preferred_columns + [
                    col for col in timetable_df.columns if col not in preferred_columns
                ]
                timetable_df = timetable_df[reordered_columns]

                # Calculate the time difference at each station
                timetable_df['actualTime'] = pd.to_datetime(timetable_df['actualTime'], errors='coerce')
                station_times = timetable_df.groupby('stationShortCode').apply(
                    lambda group: {
                        "stationName": group["stationName"].iloc[0],
                        "arrival": group.loc[group["type"] == "ARRIVAL", "actualTime"].iloc[0] if not group.loc[group["type"] == "ARRIVAL", "actualTime"].empty else None,
                        "departure": group.loc[group["type"] == "DEPARTURE", "actualTime"].iloc[0] if not group.loc[group["type"] == "DEPARTURE", "actualTime"].empty else None,
                    }
                )

                try:
                    # Load train station data
                    train_stations = pd.read_csv(train_stations_file)

                    # Merge timetable_df with train_stations to get latitude and longitude
                    timetable_with_coords = timetable_df.merge(
                        train_stations[["stationShortCode", "latitude", "longitude"]],
                        on="stationShortCode",
                        how="left"
                    )

                    # Calculate the center of the map between the first and last stations
                    first_station = timetable_with_coords.iloc[0]
                    last_station = timetable_with_coords.iloc[-1]
                    center_lat = (first_station["latitude"] + last_station["latitude"]) / 2
                    center_lon = (first_station["longitude"] + last_station["longitude"]) / 2

                    # Initialize a folium map centered on the midpoint
                    train_map = folium.Map(location=[center_lat, center_lon], zoom_start=7)

                    # Add markers for each station
                    for station_code, times in station_times.items():
                        station_coords = train_stations[train_stations['stationShortCode'] == station_code]
                        if not station_coords.empty:
                            latitude = station_coords['latitude'].iloc[0]
                            longitude = station_coords['longitude'].iloc[0]

                            arrival_time = times['arrival']
                            departure_time = times['departure']
                            time_difference = (departure_time - arrival_time).total_seconds() / 60 if arrival_time and departure_time else None

                            # Determine marker color based on the rules
                            if time_difference is None:
                                color = "blue"  # Default color if no time difference is available
                            elif time_difference == 0:
                                color = "blue"
                            elif time_difference > 0:
                                color = "red"
                            else:  # time_difference < 0
                                color = "green"

                            # Prepare popup information
                            if time_difference is not None:
                                popup_info = (
                                    f"Station: {times['stationName']}<br>"
                                    f"Arrival: {arrival_time}<br>"
                                    f"Departure: {departure_time}<br>"
                                    f"Time at station: {time_difference:.2f} minutes"
                                )
                            else:
                                popup_info = (
                                    f"Station: {times['stationName']}<br>"
                                    f"Arrival: {arrival_time}<br>"
                                    f"Departure: {departure_time}"
                                )

                            # Add a marker with the determined color
                            folium.Marker(
                                location=[latitude, longitude],
                                popup=popup_info,
                                icon=folium.Icon(icon="train", prefix="fa", color=color)
                            ).add_to(train_map)

                    # Display the map
                    st_folium(train_map, height=500, width=800, returned_objects=[])

                except Exception as e:
                    st.error(f"An error occurred while plotting the map: {e}")

                # Display the timetable after the map
                st.subheader("Time Table for Selected Train")
                st.dataframe(timetable_df, height=800, width=1000)

            else:
                st.warning("No timetable data available for the selected train.")

        else:
            st.error("The train_data DataFrame is empty.")
    except Exception as e:
        st.error(f"An error occurred while processing the CSV file: {e}")
