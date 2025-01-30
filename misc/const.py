# PARAMETERS
START_DATE = "2024-12-01" # YYYY-MM-DD
END_DATE = "2024-12-02" # YYYY-MM-DD
CATEGORY = "All Categories" 
OPERATOR = "All Operators"

# 
FMI_BBOX = "18,55,35,75" # Bounding box for Finland

# URLs for the Finnish Meteorological Institute API
FMI_OBSERVATIONS = "fmi::observations::weather::multipointcoverage"
FMI_EMS = "fmi::ef::stations"

# URLs for the Finnish Railway API
FIN_RAILWAY_BASE_URL = "https://rata.digitraffic.fi/api/v1"
FIN_RAILWAY_STATIONS = "/metadata/stations"
FIN_RAILWAY_TRAIN_CAT = "/metadata/train-categories"
FIN_RAILWAY_ALL_TRAINS = "/trains"
FIN_RAILWAY_TRAIN_TRACKING = "/train-tracking"

# CSVs
FOLDER_NAME = "output_data"
CSV_TRAIN_STATIONS = "train_station.csv"
CSV_TRAIN_CATEGORIES = "train_categories.csv"
CSV_ALL_TRAINS = "all_trains_data.csv"
CSV_FMI = "fmi_weather_observations.csv"
CSV_FMI_EMS = "fmi_ems_stations.csv"

