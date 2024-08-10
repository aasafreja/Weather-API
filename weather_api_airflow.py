import requests
from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
API_KEY = os.getenv("API_KEY")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER_DB")
PASSWORD = os.getenv("PASS")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
EMAIL = os.getenv("EMAIL")

print(EMAIL)


# List of European capitals
EUROPEAN_CAPITALS = [
    'Vienna', 'Brussels', 'Bucharest', 'Sofia', 'Berlin', 'Copenhagen',
    'Athens', 'Budapest', 'Rome', 'Madrid', 'Oslo', 'Lisbon', 'Ljubljana',
    'Valletta', 'Amsterdam', 'Warsaw', 'Helsinki', 'Stockholm', 'Riga',
    'Vilnius', 'Tallinn', 'Zagreb', 'Sarajevo', 'Podgorica', 'Pristina',
    'Skopje', 'Belgrade', 'Kyiv', 'Chisinau', 'Minsk'
]


def fetch_weather_data(city):
    try:
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        weather_data = {
            'city': data['name'],
            'country': data['sys']['country'],
            'longitude': data['coord']['lon'],
            'latitude': data['coord']['lat'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'sea_level': data['main'].get('sea_level'),
            'grnd_level': data['main'].get('grnd_level'),
            'visibility': data.get('visibility'),
            'wind_speed': data['wind']['speed'],
            'wind_direction': data['wind']['deg'],
            'wind_gust': data['wind'].get('gust'),
            'cloudiness': data['clouds']['all'],
            'rain_1h': data.get('rain', {}).get('1h', 0),
            'snow_1h': data.get('snow', {}).get('1h', 0),
            'weather_id': data['weather'][0]['id'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).strftime('%Y-%m-%d %H:%M:%S'),
            'sunset': datetime.fromtimestamp(data['sys']['sunset']).strftime('%Y-%m-%d %H:%M:%S'),
            'description': data['weather'][0]['description'],
            'feels_like': data['main']['feels_like'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return weather_data
    except requests.RequestException as e:
        print(f"Request Error for {city}: {e}")
        return None

def create_table():
    try:
        with psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS weather_europe (
                        city VARCHAR(100),
                        country VARCHAR(2),
                        longitude REAL,
                        latitude REAL,
                        temperature REAL,
                        humidity REAL,
                        pressure REAL,
                        sea_level REAL,
                        grnd_level REAL,
                        visibility INTEGER,
                        wind_speed REAL,
                        wind_direction REAL,
                        wind_gust REAL,
                        cloudiness INTEGER,
                        rain_1h REAL,
                        snow_1h REAL,
                        weather_main VARCHAR(100),
                        weather_description TEXT,
                        sunrise TIMESTAMP,
                        sunset TIMESTAMP,
                        description TEXT,
                        feels_like REAL,
                        timestamp TIMESTAMP
                    )
                ''')
    except psycopg2.Error as e:
        print(f"Database Error: {e}")

def truncate_table():
    try:
        with psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE weather_europe')
                conn.commit()
    except psycopg2.Error as e:
        print(f"Database Error: {e}")

def insert_weather_data(data):
    try:
        with psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO weather_europe (city, country, longitude, latitude, temperature, humidity, pressure,sea_level, grnd_level, visibility, wind_speed, wind_direction, 
                                         wind_gust, cloudiness, rain_1h, snow_1h, weather_main, weather_description, 
                                         sunrise, sunset, description, feels_like, timestamp)
                    VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (data['city'],data['country'], data['longitude'], data['latitude'], data['temperature'], data['humidity'], data['pressure'],data['sea_level'], data['grnd_level'],
                    data['visibility'], data['wind_speed'], data['wind_direction'], data['wind_gust'], data['cloudiness'],
                    data['rain_1h'], data['snow_1h'], data['weather_main'], data['weather_description'],
                    data['sunrise'], data['sunset'], data['description'],data['feels_like'], data['timestamp']))
                conn.commit()
    except psycopg2.Error as e:
        print(f"Database Error: {e}")

def fetch_data_from_db():
    try:
        with psycopg2.connect(dbname=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM weather_europe')
                rows = cur.fetchall()
        return rows
    except psycopg2.Error as e:
        print(f"Database Error: {e}")
        return []

def main_task():
    
    create_table()
    for city in EUROPEAN_CAPITALS:
        weather_data = fetch_weather_data(city)
        if weather_data:
            print(f"Fetched Weather Data for {city}:", weather_data)
            insert_weather_data(weather_data)
    rows = fetch_data_from_db()
    print("Data in Database:")
    for row in rows:
        print(row)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': EMAIL,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='A DAG to fetch weather data and load into a PostgreSQL database',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2024, 8, 1),
    catchup=False,
)

t1_main_task = PythonOperator(
    task_id='run_main',
    python_callable=main_task,
    dag=dag,
)

t1_main_task