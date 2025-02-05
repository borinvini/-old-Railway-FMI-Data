import streamlit as st
import pandas as pd
import os
import ast

from misc.misc_functions import save_dataframe_to_csv

# Configuration: change these as needed
FOLDER_NAME = "output_data"  # Folder where your CSV files are stored
FILE_PREFIX = "cross_data_"
FILE_SUFFIX = ".csv"

st.title("Process and Save Final Cross Data for All Months")

processed_files = []

# Loop over all 12 months.
for month in range(1, 13):
    month_str = str(month).zfill(2)
    file_name = f"{FILE_PREFIX}{month_str}{FILE_SUFFIX}"
    file_path = os.path.join(os.getcwd(), FOLDER_NAME, file_name)
    
    st.write(f"**Processing file:** `{file_name}`")
    
    # Load the CSV file and preprocess.
    try:
        df = pd.read_csv(file_path, parse_dates=["departureDate"])
        if "departureMonth" in df.columns:
            df = df.drop("departureMonth", axis=1)
        df = df.sort_values(by="trainNumber")
    except FileNotFoundError:
        st.error(f"File '{file_name}' not found in folder '{FOLDER_NAME}'. Skipping...")
        continue
    except Exception as e:
        st.error(f"Error loading file '{file_name}': {e}. Skipping...")
        continue

    # ----------------------------------------------------------
    # Extract nested data from the "timeTableRows" column.
    # ----------------------------------------------------------
    cross_records = []
    for index, row in df.iterrows():
        ttr = row.get("timeTableRows", None)
        if ttr is None or pd.isnull(ttr):
            continue  # Skip empty values
        # If the value is a string, attempt to convert it.
        if isinstance(ttr, str):
            try:
                # Replace unquoted 'nan' with 'None' so ast.literal_eval can parse it.
                ttr_fixed = ttr.replace("nan", "None")
                stops = ast.literal_eval(ttr_fixed)
            except Exception as e:
                st.write(f"Error parsing timeTableRows in row {index}: {e}")
                continue
        else:
            stops = ttr  # Assume it's already a Python object
        # Ensure stops is a list; if not, wrap it in a list.
        if not isinstance(stops, list):
            stops = [stops]
        for stop in stops:
            cross_records.append(stop)

    # Create a DataFrame from the extracted dictionaries.
    cross_df = pd.DataFrame(cross_records)

    # ----------------------------------------------------------
    # Keep only the columns: "differenceInMinutes", "cancelled", and "weather_conditions".
    # Rename 'weather_observations' to 'weather_conditions' if it exists.
    # ----------------------------------------------------------
    if "weather_observations" in cross_df.columns:
        cross_df = cross_df.rename(columns={"weather_observations": "weather_conditions"})

    expected_cols = ["differenceInMinutes", "cancelled", "weather_conditions"]
    if set(expected_cols).issubset(cross_df.columns):
        cross_df = cross_df[expected_cols]
    else:
        st.write(f"Warning: Not all expected columns are available in {file_name}. Available columns:")
        st.write(list(cross_df.columns))

    # ----------------------------------------------------------
    # Expand the 'weather_conditions' dictionaries into separate columns,
    # dropping the unwanted keys: "closest_ems" and "Present weather (auto)".
    # ----------------------------------------------------------
    if "weather_conditions" in cross_df.columns:
        weather_df = cross_df["weather_conditions"].apply(pd.Series)
        weather_df = weather_df.drop(columns=["closest_ems", "Present weather (auto)"], errors="ignore")
        cross_df = cross_df.drop("weather_conditions", axis=1).join(weather_df)

    # ----------------------------------------------------------
    # Reorder columns: differenceInMinutes, cancelled, then the expanded weather columns.
    # ----------------------------------------------------------
    cols_order = ["differenceInMinutes", "cancelled"] + [col for col in cross_df.columns if col not in ["differenceInMinutes", "cancelled"]]
    cross_df = cross_df[cols_order]

    # Save the final DataFrame to a CSV file.
    output_filename = f"final_corr_data_{month_str}.csv"
    save_dataframe_to_csv(cross_df, output_filename)
    processed_files.append(output_filename)
    st.write(f"Finished processing and saved `{output_filename}`.")

st.write("**All files processed. Final output files:**")
st.write(processed_files)
