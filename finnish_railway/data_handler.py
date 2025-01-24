import requests
import pandas as pd
import os

from misc.const import FIN_RAILWAY_ALL_TRAINS, FIN_RAILWAY_BASE_URL

def load_railway_metadata(url):
    """
    Load metadata from the given API URL and return it as a DataFrame.

    Args:
        url (str): The API endpoint to fetch the metadata.

    Returns:
        pd.DataFrame: DataFrame containing the metadata.
    """
    try:
        # Fetch the metadata
        print(f"Loading metadata from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Convert the JSON data into a DataFrame
        df = pd.DataFrame(data)

        if df.empty:
            print("Metadata is empty. Please check the API or data source.")
            return pd.DataFrame()

        print(f"Metadata successfully loaded from {url}")
        return df

    except requests.RequestException as e:
        print(f"Error fetching metadata: {e}")
        return pd.DataFrame()

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()
    

def get_data(endpoint, params=None):
    """
    Fetch data from the API endpoint with optional parameters.

    Args:
        endpoint (str): The API endpoint path.
        params (dict, optional): Query parameters for the request.

    Returns:
        dict: JSON response from the API.
    """
    url = FIN_RAILWAY_BASE_URL + endpoint
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return None
    
def get_trains_by_date(date_str):
    """
    Fetch train data for a given date and return it as a DataFrame.

    Args:
        date_str (str): The date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame containing all trains for the given date.
    """
    from misc.const import FIN_RAILWAY_ALL_TRAINS
    endpoint = f"{FIN_RAILWAY_ALL_TRAINS}/{date_str}"
    trains = get_data(endpoint)
    if trains:
        return pd.DataFrame(trains)
    else:
        print(f"No train data found for {date_str}")
        return pd.DataFrame()