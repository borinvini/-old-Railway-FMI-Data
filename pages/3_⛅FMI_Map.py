import streamlit as st
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from misc.const import FMI_BBOX

# Load bounding box from const.py
bbox_values = list(map(float, FMI_BBOX.split(',')))
lower_left_lon, lower_left_lat, upper_right_lon, upper_right_lat = bbox_values

# Streamlit UI
st.title("Bounding Box Visualization")
st.markdown("""
This Streamlit app visualizes a bounding box using the Basemap library. The red rectangle represents the bounding box, with marked lower-left (blue) and upper-right (green) corners.
""")

# Create the plot
fig, ax = plt.subplots(figsize=(8, 6))

# Initialize a Basemap instance
m = Basemap(projection='cyl',
            llcrnrlon=lower_left_lon, llcrnrlat=lower_left_lat,
            urcrnrlon=upper_right_lon, urcrnrlat=upper_right_lat,
            resolution='i', ax=ax)

# Draw map boundaries and features
m.drawcoastlines()
m.drawcountries()
m.drawparallels(range(-90, 91, 5), labels=[1, 0, 0, 0])
m.drawmeridians(range(-180, 181, 5), labels=[0, 0, 0, 1])

# Plot the bounding box as a rectangle
bbox_lon = [lower_left_lon, upper_right_lon, upper_right_lon, lower_left_lon, lower_left_lon]
bbox_lat = [lower_left_lat, lower_left_lat, upper_right_lat, upper_right_lat, lower_left_lat]

ax.plot(bbox_lon, bbox_lat, color='red', linewidth=2, label='Bounding Box')
ax.scatter(lower_left_lon, lower_left_lat, color='blue', label='Lower Left Corner')
ax.scatter(upper_right_lon, upper_right_lat, color='green', label='Upper Right Corner')

# Add legend and title
ax.legend()
ax.set_title("Bounding Box Visualization")

# Show the plot in Streamlit
st.pyplot(fig)
