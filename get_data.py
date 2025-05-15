import os
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests
from datetime import datetime

def get_current_weather_and_pm10():
    from datetime import timedelta

    # Open Meteo setup
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    weather_url = "https://api.open-meteo.com/v1/forecast"
    today = datetime.utcnow().date()
    start_date = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    weather_params = {
        "latitude": 54.40359618668799,
        "longitude": 18.668799,
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "pressure_msl"],
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "auto"
    }

    try:
        weather_response = openmeteo.weather_api(weather_url, params=weather_params)[0]
        hourly = weather_response.Hourly()
        timestamps = pd.to_datetime(hourly.Time(), unit='s')

        df_weather = pd.DataFrame({
            "time": timestamps,
            "temperature": hourly.Variables(0).ValuesAsNumpy(),
            "humidity": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation": hourly.Variables(2).ValuesAsNumpy(),
            "wind_speed": hourly.Variables(3).ValuesAsNumpy(),
            "pressure": hourly.Variables(4).ValuesAsNumpy()
        })

        df_weather['date'] = df_weather['time'].dt.date
        df_today = df_weather[df_weather['date'] == today]

        temperature = df_today['temperature'].mean()
        humidity = df_today['humidity'].mean()
        precipitation = df_today['precipitation'].sum()
        wind_speed = df_today['wind_speed'].mean()
        pressure = df_today['pressure'].mean()

    except Exception as e:
        print(f"Error with downloading weather data: {e}")
        return pd.DataFrame()

    # PM10 jak wcześniej
    station_id = 731
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

    df_pm10['date_only'] = df_pm10['date'].dt.date
    df_today_pm10 = df_pm10[df_pm10['date_only'] == today]
    pm10_value = df_today_pm10['value'].mean() if not df_today_pm10.empty else None

    # Zwróć zaktualizowane dane
    result = pd.DataFrame([{
        "Data": datetime.now().strftime("%Y-%m-%d"),
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
