import pandas as pd
import os
import streamlit as st
import time

from misc.const import CATEGORY, CSV_ALL_TRAINS, FOLDER_NAME, OPERATOR, START_DATE

# Title of the Streamlit App
st.title("Train Data Viewer")

# Path to the CSV file
csv_file = os.path.join(FOLDER_NAME, CSV_ALL_TRAINS)  # Adjust path if necessary

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

        # Introduce a delay of 3 seconds
        time.sleep(3)

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

                # Display the timetable rows for the selected train
                st.subheader("Time Table for Selected Train")
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

                # Display the timetable
                st.dataframe(timetable_df, height=800, width=1000)
            else:
                st.warning("No timetable data available for the selected train.")

        else:
            st.error("The train_data DataFrame is empty.")
    except Exception as e:
        st.error(f"An error occurred while processing the CSV file: {e}")
