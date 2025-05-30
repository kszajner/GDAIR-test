import pandas as pd
import holidays
from datetime import datetime
import requests
import joblib
import os

# Get the Discord webhook - to send notifications to server
webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
if not webhook_url:
    raise ValueError("Webhook URL not set in environment variables")

def get_season(date):
    """
    Determine the season based on the given date.
    """
    if (date.month == 12 and date.day >= 21) or (date.month in [1, 2]) or (date.month == 3 and date.day <= 20):
        return 4  # Winter
    elif (date.month == 3 and date.day >= 21) or (date.month in [4, 5]) or (date.month == 6 and date.day <= 20):
        return 1  # Spring
    elif (date.month == 6 and date.day >= 21) or (date.month in [7, 8]) or (date.month == 9 and date.day <= 20):
        return 2  # Summer
    else:
        return 3  # Autumn

def get_last_observations_transformed(filename):
    """
    Fetch the last three observations from the data file, transforming them into the required format.
    """
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        print(f"File {filename} does not exist.")
        return pd.DataFrame()

    if df.empty:
        print("File is empty.")
        return pd.DataFrame()

    # Convert 'Data' column to datetime
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

    # Get the last 3 rows
    last_3 = df.tail(3)

    if last_3.empty:
        print("Not enough data.")
        return pd.DataFrame()

    # Handle holidays
    pl_holidays = holidays.Poland(years=range(2015, 2030))

    # List to store transformed data
    transformed_list = []

    for index, row in last_3.iterrows():
        date = row['Data']
        transformed_list.append({
            "Month": date.month,
            "Tavg": row["temperature"],
            "Pavg": row["precipitation"],
            "Wavg": row["wind speed"],
            "Huavg": row["relative humidity"],
            "Pravg": row["pressure"],
            "PM10": row["pm10"],
            "IsWeekend": int(date.weekday() >= 5),
            "IsHoliday": int(date.date() in pl_holidays),
            "Season": get_season(date)
        })

    # Create a DataFrame from the transformed data
    transformed_df = pd.DataFrame(transformed_list)

    return transformed_df

def create_sequence(df):
    """
    Create sequences of 3 consecutive observations from the DataFrame.
    """
    sequences = []
    i = 0
    while i < len(df):
        seq = df.iloc[i:i + 3].values.flatten()
        sequences.append(seq)
        i += 3
    return pd.DataFrame(sequences)

def send_to_discord(message, webhook_url):
    """
    Send a message to Discord using the provided webhook URL.
    """
    data = {
        "content": message
    }
    response = requests.post(webhook_url, json=data)
    if response.status_code != 204:
        print(f"Failed to send message. Status code: {response.status_code}")
    else:
        print("Message sent successfully!")

def predict_from_last_sequence(df, model_path="model.pkl", webhook_url=webhook_url):
    """
    Make a prediction from the last 3 observations and send the result to Discord.
    """
    # Column names
    original_columns = [
        "Month", "Tavg", "Pavg", "Wavg", "Huavg", "Pravg", "PM10", "IsWeekend", "IsHoliday", "Season"
    ]
    all_columns = (
        original_columns +
        [f"{col}_2" for col in original_columns] +
        [f"{col}_3" for col in original_columns]
    )

    # Check if there are enough rows to make a prediction
    if len(df) < 3:
        print("Not enough data.")
        return

    # Create sequence
    seq_df = create_sequence(df)
    seq_df.columns = all_columns

    # Load the model
    model = joblib.load(model_path)

    # Make prediction
    probabilities = model.predict_proba(seq_df)

    # Probability of class 1 (high PM10 level)
    prob_class_1 = probabilities[:, 1]

    # Send results
    risk_probability = prob_class_1[0]
    confidence_pct = round(risk_probability * 100, 1)
    # Build contextual forecast message
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    wind = seq_df["Wavg_3"].iloc[0]
    pressure = seq_df["Pravg_3"].iloc[0]
    temp = seq_df["Tavg_3"].iloc[0]
    current_pm10 = seq_df["PM10_3"].iloc[0]
    
    message = f"**PM10 Air Quality Forecast - {timestamp}**\n"
    message += f"📊 **Exceedance Probability:** {confidence_pct}% (threshold: 50 μg/m³)\n"
    message += f"🌡️ **Current Conditions:** {temp:.1f}°C, {wind:.1f} m/s wind, {pressure:.0f} hPa\n"
    message += f"💨 **Baseline PM10:** {current_pm10:.0f} μg/m³\n\n"
    
    # Risk assessment with meteorological context
    if risk_probability > 0.7:
        message += f"🔴 **HIGH RISK ALERT**\n"
        message += f"Model indicates {confidence_pct}% probability of exceeding WHO daily guidelines tomorrow.\n"
        if wind < 2.0:
            message += f"⚠️ Low wind conditions ({wind:.1f} m/s) limiting dispersion.\n"
        if pressure > 1020:
            message += f"⚠️ High pressure system ({pressure:.0f} hPa) promoting accumulation.\n"
        message += f"**Recommendation:** Limit outdoor exercise, vulnerable groups should stay indoors."
        
    elif risk_probability > 0.4:
        message += f"🟡 **MODERATE RISK**\n"
        message += f"Elevated probability ({confidence_pct}%) of air quality deterioration.\n"
        message += f"**Recommendation:** Monitor conditions, sensitive individuals exercise caution."
        
    else:
        message += f"🟢 **LOW RISK**\n"
        message += f"Favorable atmospheric conditions predicted ({confidence_pct}% exceedance risk).\n"
        if wind > 4.0:
            message += f"✅ Good wind dispersion ({wind:.1f} m/s) supporting air quality.\n"
        message += f"**Outlook:** Air quality should remain within acceptable limits."
    
    message += f"\n*Model: RandomForest | Data: 72h moving window*"

    # Send result to Discord
    send_to_discord(message, webhook_url)

def main():
    """
    Main function to load data, make prediction, and send it to Discord.
    """
    df = get_last_observations_transformed("current_data.csv")
    if not df.empty:
        predict_from_last_sequence(df, model_path="model.pkl", webhook_url=webhook_url)

# Run the main function
if __name__ == "__main__":
    main()
