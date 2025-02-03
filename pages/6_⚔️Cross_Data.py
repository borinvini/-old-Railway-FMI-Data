import pandas as pd
import os
import streamlit as st
import json

from misc.const import CSV_CROSS_DATA, FOLDER_NAME

# Title of the Streamlit App
st.title("📊 Train & Weather Data Viewer")

# Add a selectbox to choose a month (from "01" to "12")
selected_month = st.selectbox(
    "Select Month",
    options=[str(i).zfill(2) for i in range(1, 13)],
    index=0  # default to "01"
)

# Build the file path based on the selected month.
# For example, if selected_month is "01", then the file will be "cross_data_01.csv"
csv_file = os.path.join(FOLDER_NAME, f"cross_data_{selected_month}.csv")


try:
    # Load the DataFrame from the CSV file
    df = pd.read_csv(csv_file, parse_dates=["departureDate"])

    # Check if the DataFrame is not empty
    if not df.empty:
        # Extract unique departure dates from the dataset
        df["departureDate"] = df["departureDate"].dt.strftime("%Y-%m-%d")  # Convert to date format
        unique_dates = sorted(df["departureDate"].unique())  # Sort dates

        # Add a selectbox to choose a departure date
        selected_date = st.selectbox("📅 Select a departure date:", unique_dates)

        # Filter data based on selected departure date
        filtered_data = df[df["departureDate"] == selected_date]

        # Subheader for the table
        st.subheader(f"🚆 Train & Weather Data for {selected_date}")

        # Display the filtered DataFrame
        st.dataframe(filtered_data, height=800, width=1000)

        # Additional information about the DataFrame
        st.write(f"📌 **Total Rows for {selected_date}:** {filtered_data.shape[0]}")
        st.write(f"📍 **Total Columns:** {filtered_data.shape[1]}")

        # Selectbox for train numbers of the selected departure date
        train_numbers = sorted(filtered_data["trainNumber"].unique())
        selected_train = st.selectbox("🚆 Select a train number:", train_numbers)

        # Display the timeTableRows for the selected train
        train_data = filtered_data[filtered_data["trainNumber"] == selected_train]

        if "timeTableRows" in train_data.columns:
            try:
                # Extract timetable data as a string
                timetable_info = train_data["timeTableRows"].values[0]

                # 🔹 Fix potential formatting issues
                timetable_info = (
                    timetable_info.replace("'", "\"")
                    .replace("None", "null")
                    .replace("True", "true").replace("False", "false")
                    .replace("nan", "null")
                )

                # 🔹 Try to safely parse JSON
                timetable_data = json.loads(timetable_info)

                # Convert to DataFrame
                timetable_df = pd.DataFrame(timetable_data)

                # Subheader for timetable
                st.subheader(f"📍 Timetable for Train {selected_train}")
                
                # Display DataFrame
                st.dataframe(timetable_df, height=600, width=1000)

                # Extract weather observations from each row
                weather_data = []
                for row in timetable_data:
                    weather = row.get("weather_observations", {})
                    weather["stationShortCode"] = row.get("stationShortCode", "Unknown")  # Add station name
                    weather["scheduledTime"] = row.get("scheduledTime", "Unknown")  # Add scheduled time
                    weather_data.append(weather)

                # Convert extracted weather data into a DataFrame
                weather_df = pd.DataFrame(weather_data)

                if not weather_df.empty:
                    # **Reorder columns: stationShortCode first, scheduledTime second, rest follows**
                    column_order = ["stationShortCode", "scheduledTime"] + [
                        col for col in weather_df.columns if col not in ["stationShortCode", "scheduledTime"]
                    ]
                    weather_df = weather_df[column_order]

                    # Subheader for weather observations
                    st.subheader(f"🌤️ Weather Observations for Train {selected_train}")

                    # Display Weather DataFrame
                    st.dataframe(weather_df, height=400, width=1000)
                else:
                    st.warning("No weather observations available for this train.")

            except json.JSONDecodeError:
                st.error("🚨 JSON parsing failed! Displaying raw data for debugging:")
                st.text_area("Raw timetable data:", timetable_info, height=300)
            except Exception as e:
                st.error(f"❌ Unexpected error parsing timetable data: {e}")
        else:
            st.warning("No timetable data available for the selected train.")

    else:
        st.error("The dataset is empty. No data to display.")

except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
