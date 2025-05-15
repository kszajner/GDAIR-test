import os
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests
from datetime import datetime

def get_current_weather_and_pm10():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    weather_url = "https://api.open-meteo.com/v1/forecast"
    today = datetime.now().date()

    weather_params = {
        "latitude": 54.4036,
        "longitude": 18.6688,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation", "pressure_msl"],
        "start_date": today.isoformat(),
        "end_date": today.isoformat(),
        "timezone": "Europe/Warsaw"
    }

    try:
        weather_response = openmeteo.weather_api(weather_url, params=weather_params)[0]
        hourly = weather_response.Hourly()
        hours = pd.date_range(start=today, periods=24, freq='H')

        data = {
            "temperature": hourly.Variables(0).ValuesAsNumpy(),
            "humidity": hourly.Variables(1).ValuesAsNumpy(),
            "wind_speed": hourly.Variables(2).ValuesAsNumpy(),
            "precipitation": hourly.Variables(3).ValuesAsNumpy(),
            "pressure": hourly.Variables(4).ValuesAsNumpy()
        }

        df_weather = pd.DataFrame(data, index=hours)
        df_weather.dropna(inplace=True)

        avg_temp = df_weather["temperature"].mean()
        avg_humidity = df_weather["humidity"].mean()
        avg_wind = df_weather["wind_speed"].mean()
        sum_precip = df_weather["precipitation"].sum()
        avg_pressure = df_weather["pressure"].mean()

    except Exception as e:
        print(f"Błąd przy pobieraniu danych pogodowych: {e}")
        return pd.DataFrame()

    # PM10
    try:
        import requests
        station_id = 731
        base_url = "https://api.gios.gov.pl/pjp-api/rest"
        sensors = requests.get(f"{base_url}/station/sensors/{station_id}").json()
        pm10_sensor = next((s for s in sensors if s.get("param", {}).get("paramCode") == "PM10"), None)

        if not pm10_sensor:
            raise ValueError("Brak sensora PM10")

        data_json = requests.get(f"{base_url}/data/getData/{pm10_sensor['id']}").json()
        df_pm10 = pd.DataFrame(data_json.get("values", []))
        df_pm10['date'] = pd.to_datetime(df_pm10['date'], errors='coerce')
        df_pm10['value'] = pd.to_numeric(df_pm10['value'], errors='coerce')
        df_pm10.dropna(subset=['value'], inplace=True)
        df_pm10['date_only'] = df_pm10['date'].dt.date
        df_today = df_pm10[df_pm10['date_only'] == today]
        pm10_value = df_today['value'].mean() if not df_today.empty else None
    except Exception as e:
        print(f"Błąd PM10: {e}")
        pm10_value = None

    # Zwróć dane
    result = pd.DataFrame([{
        "Data": datetime.now().replace(microsecond=0),
        "wind speed": avg_wind,
        "temperature": avg_temp,
        "relative humidity": avg_humidity,
        "precipitation": sum_precip,
        "pressure": avg_pressure,
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
