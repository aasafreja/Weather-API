# Weather Data ETL Pipeline with Airflow and PostgreSQL

## Overview

This project is a Python-based ETL (Extract, Transform, Load) pipeline that fetches weather data for European capitals from the OpenWeatherMap API, stores the data in a PostgreSQL database, and uses Apache Airflow to orchestrate the data loading process. The project also includes a temperature heatmap visualization.

## Project Structure

- weather_etl_functions.py: Contains functions to fetch weather data from OpenWeatherMap API, create a PostgreSQL table, insert data, and retrieve data from the database.
- weather_etl_dag.py: Defines an Apache Airflow DAG to automate the ETL process.
- weather_vis.py: Generates a temperature heatmap visualization (not provided in the code snippet but typically would be included).

## Requirements

- Python 3.7+
- PostgreSQL
- Apache Airflow
- Python libraries: requests, psycopg2, dotenv, apache-airflow, datetime

## Setting Up Environment Variables

This project uses environment variables to manage configuration settings. To get started, you'll need to create a `.env` file in the root directory of the project with the necessary variables.

### Steps to Configure

1. **Create a `.env` File**

   In the root directory of the project, create a file named `.env`.

2. **Add Environment Variables**

   Add the required environment variables to the `.env` file. Below is an example of the content you should include:

   ```env
   DATABASE=your-database-url
   API_KEY=your-api-key
   PORT=3000
   
### Getting an API Key from OpenWeatherMap

To use the weather data API from OpenWeatherMap, you need an API key. Follow these steps to obtain one:

1. **Sign Up / Log In**
   - Visit [OpenWeatherMap](https://openweathermap.org/).
   - Click on **Sign In** in the upper right corner.
   - If you don’t have an account, click on **Sign Up** and complete the registration process.

2. **Access API Keys**
   - Once logged in, click on your profile icon in the top right corner and select **API keys** from the dropdown menu.

3. **Generate a New API Key**
   - On the API keys page, click on **Create Key**.
   - Enter a descriptive name for your API key (e.g., "MyProjectKey") and click **Generate**.

4. **Copy Your API Key**
   - After the key is generated, copy it from the list. You’ll need this key to make API requests.

5. **Store the API Key**
   - Create a `.env` file in your project's root directory and add your API key as follows:

   ```env
   API_KEY=your-api-key-here



