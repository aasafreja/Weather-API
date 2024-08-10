import pandas as pd
from sqlalchemy import create_engine
import os
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

USER = os.getenv("USER_DB")
PASS = os.getenv("PASS")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DATABASE = os.getenv("DATABASE")


# Create an engine to connect to the PostgreSQL database
engine = create_engine(f'postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DATABASE}')

# Query to get the city, temperature, and time of day data
query = "SELECT city, temperature, timestamp FROM weather_europe"

# Load the data into a pandas DataFrame
df = pd.read_sql(query, engine)

# Convert timestamp in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Extract the hour from the timestamp
df['hour'] = df['timestamp'].dt.hour

# Define time of day based on hour
conditions = [
    (df['hour'] >= 0) & (df['hour'] < 6),
    (df['hour'] >= 6) & (df['hour'] < 12),
    (df['hour'] >= 12) & (df['hour'] < 18),
    (df['hour'] >= 18) & (df['hour'] <= 23)
]
choices = ['Night', 'Morning', 'Afternoon', 'Evening']

df['time_of_day'] = pd.cut(df['hour'], bins=[-1, 5, 11, 17, 23], labels=choices, right=True)


# 'hour' column no longer needed
df = df.drop(columns=['hour'])

# Pivot the DataFrame to create a matrix of cities vs. time_of_day with temperature as values
df_pivot = df.pivot_table(index='city', columns='time_of_day', values='temperature', aggfunc='mean')


# Define the path for saving the file
file_path = '/Users/Macbook Pro M1/Desktop/temperature_heatmap.png'

# Check if the file exists
if os.path.exists(file_path):
    print(f"File {file_path} already exists and will be overwritten.")

# Create the heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(df_pivot, annot=True, cmap="coolwarm", linewidths=.5, fmt=".1f")

# Add title and labels
plt.title('Temperature Variations Across Cities and Time of Day')
plt.xlabel('Time of Day')
plt.ylabel('City')

# Save and show the heatmap
plt.savefig(file_path, format='png', dpi=300, bbox_inches='tight')
plt.show(block=True)
