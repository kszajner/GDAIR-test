import os
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests
from datetime import datetime

def get_current_weather_and_pm10():
    """
    Fetch current weather data and PM10 data, and return the results as a DataFrame.
    """
    # Open Meteo
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)  # Cache for 1 hour
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)  # Retry up to 5 times with exponential backoff
    openmeteo = openmeteo_requests.Client(session=retry_session)

    weather_url = "https://api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": 54.40359618668799,
        "longitude": 18.668799,
        "current": ["wind_speed_10m", "temperature_2m", "relative_humidity_2m", "precipitation", "pressure_msl"]
    }

    try:
        weather_response = openmeteo.weather_api(weather_url, params=weather_params)[0]
        current = weather_response.Current()

        current_time = datetime.utcfromtimestamp(current.Time())
        wind_speed = current.Variables(0).Value()
        temperature = current.Variables(1).Value()
        humidity = current.Variables(2).Value()
        precipitation = current.Variables(3).Value()
        pressure = current.Variables(4).Value()
    except Exception as e:
        print(f"Error with downloading the data: {e}")
        return pd.DataFrame()

    # PM10
    station_id = 731  # Gdańsk Station ID
    base_url = "https://api.gios.gov.pl/pjp-api/rest"

    try:
        sensors_resp = requests.get(f"{base_url}/station/sensors/{station_id}")
        sensors_resp.raise_for_status()
        sensors = sensors_resp.json()
    except Exception as e:
        print(f"Error getting sensors: {e}")
        return pd.DataFrame()

    pm10_sensor = next((s for s in sensors if s.get("param", {}).get("paramCode") == "PM10"), None)
    if not pm10_sensor:
        print("Did not find PM10 sensor.")
        return pd.DataFrame()

    try:
        data_resp = requests.get(f"{base_url}/data/getData/{pm10_sensor['id']}")
        data_resp.raise_for_status()
        data_json = data_resp.json()
    except Exception as e:
        print(f"Error with PM10: {e}")
        return pd.DataFrame()

    values = data_json.get("values", [])
    df_pm10 = pd.DataFrame(values)
    df_pm10['date'] = pd.to_datetime(df_pm10['date'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
    df_pm10['value'] = pd.to_numeric(df_pm10['value'], errors='coerce')
    df_pm10 = df_pm10.dropna(subset=['value'])

    # Filter for today's PM10 data
    today = datetime.now().date()
    df_pm10['date_only'] = df_pm10['date'].dt.date
    df_today = df_pm10[df_pm10['date_only'] == today]

    if df_today.empty:
        print("No PM10 data for today.")
        pm10_value = None
    else:
        pm10_value = df_today['value'].mean()

    # Combine results into a single row DataFrame
    result = pd.DataFrame([{
        "Data": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "wind speed": wind_speed,
        "temperature": temperature,
        "relative humidity": humidity,
        "precipitation": precipitation,
        "pressure": pressure,
        "pm10": pm10_value
    }])

    return result

def save_to_csv(df, filename="current_data.csv"):
    """
    Save the DataFrame to a CSV file, appending to the file if it exists.
    """
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)

def main():
    """
    Main function that fetches weather and PM10 data, then saves it to a CSV.
    """
    df = get_current_weather_and_pm10()
    if not df.empty:
        save_to_csv(df)

# Run the main function
if __name__ == "__main__":
    main()