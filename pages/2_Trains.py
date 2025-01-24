import pandas as pd
import os
import streamlit as st

from misc.const import CSV_ALL_TRAINS, FOLDER_NAME

# Title of the Streamlit App
st.title("Train Data Viewer")

# Path to the CSV file
csv_file = os.path.join(FOLDER_NAME, CSV_ALL_TRAINS)  # Adjust path if necessary

try:
    # Load the DataFrame from the CSV file
    train_data = pd.read_csv(csv_file)
    
    # Check if the DataFrame is not empty
    if not train_data.empty:
        # Session State to Reset Filters
        if "reset" not in st.session_state:
            st.session_state.reset = False

        # Reset Filters Button
        st.subheader("Filters")
        if st.button("Reset Filters"):
            st.session_state.reset = True  # Trigger reset

        # Apply Filters Only If Not Reset
        if not st.session_state.reset:
            # Filter by departureDate
            unique_dates = train_data["departureDate"].sort_values().unique()
            selected_date = st.selectbox("Select Departure Date", options=unique_dates, index=0)

            # Filter by trainCategory
            unique_categories = train_data["trainCategory"].sort_values().unique()
            selected_category = st.selectbox("Select Train Category", options=unique_categories, index=0)

            # Filter by cancelled status
            show_cancelled = st.checkbox("Show Only Cancelled Trains", value=False)

            # Apply filters to the DataFrame
            filtered_data = train_data[
                (train_data["departureDate"] == selected_date) &
                (train_data["trainCategory"] == selected_category) &
                ((train_data["cancelled"] == True) if show_cancelled else (train_data["cancelled"] == train_data["cancelled"]))
            ]
        else:
            # Reset to the full DataFrame
            filtered_data = train_data
            st.session_state.reset = False  # Reset the state after showing the full DataFrame

        # Limit the number of rows to display
        max_rows_to_display = 500
        st.subheader(f"Displaying First {max_rows_to_display} Rows of Train Data")

        # Display the filtered DataFrame
        st.dataframe(filtered_data.head(max_rows_to_display), height=800, width=1000)

        # Additional information about the DataFrame
        st.write("Total number of rows:", filtered_data.shape[0])
        st.write("Number of columns:", filtered_data.shape[1])
    else:
        st.error("The train_data DataFrame is empty.")
except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
