import pandas as pd
import schedule
import time
import smtplib
import mysql.connector
from sklearn.ensemble import IsolationForest
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from configuration import DB_CONFIG, SMTP_CONFIG  # Ensure you have DB and SMTP configurations

# ✅ Function to Fetch Transaction Data from MySQL
def fetch_transaction_data():
    try:
        with mysql.connector.connect(**DB_CONFIG) as conn:
            with conn.cursor(dictionary=True) as cursor:
                query = """
                SELECT id, HOUR(created_at) AS hour, amount, created_at
                FROM international_topups
                WHERE created_at >= NOW() - INTERVAL 14 DAY;
                """  # Fetch last 14 days of transactions
                cursor.execute(query)
                return pd.DataFrame(cursor.fetchall())  # Convert to DataFrame
    except mysql.connector.Error as err:
        print(f"❌ Database connection failed: {err}")
        return pd.DataFrame()  # Return empty DataFrame if error

# ✅ Function to Train ML Model & Detect Anomalies
def detect_anomalies():
    print("\n🔄 Running ML-based Transaction Anomaly Detection...")

    # Fetch transaction data
    df = fetch_transaction_data()
    if df.empty:
        print("⚠️ No transactions found for analysis.")
        return

    # ✅ Preprocessing: Remove missing values
    df = df.dropna()

    # Features used for anomaly detection
    features = df[['hour', 'amount']]

    # ✅ Train Isolation Forest Model
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(features)  # Train the model

    # ✅ Predict anomalies (-1 = anomaly, 1 = normal)
    df['anomaly'] = model.predict(features)
    df['anomaly'] = df['anomaly'].apply(lambda x: "Anomaly" if x == -1 else "Normal")

    # ✅ Filter only anomalous transactions
    anomalies = df[df['anomaly'] == "Anomaly"]

    # ✅ Calculate Anomaly Percentage
    total_transactions = len(df)
    anomaly_count = len(anomalies)
    anomaly_percentage = (anomaly_count / total_transactions) * 100

    print(f"\n📊 Total Transactions: {total_transactions}, Anomalies: {anomaly_count}, Anomaly Percentage: {anomaly_percentage:.2f}%")

    # ✅ Send Alert if Anomalies Exceed 20%
    if anomaly_percentage >= 20:
        send_email_alert(anomalies, anomaly_percentage)
    else:
        print("✅ No significant anomalies detected (Below 20%). Email not sent.")

# ✅ Function to Send Email Alerts
def send_email_alert(anomalies, anomaly_percentage):
    try:
        print("\n🚨 Sending Anomaly Alert Email...")
        subject = f"🚨 Transaction Anomaly Alert ({anomaly_percentage:.2f}% Anomalies) 🚨"
        body = f"The system detected {anomaly_percentage:.2f}% anomalies in transactions:\n\n{anomalies.to_string(index=False)}"

        msg = MIMEMultipart()
        msg["From"] = SMTP_CONFIG["sender"]
        msg["To"] = SMTP_CONFIG["receiver"]
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # ✅ Secure Connection & Send Email
        with smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"]) as server:
            server.starttls()
            server.login(SMTP_CONFIG["user"], SMTP_CONFIG["password"])
            server.send_message(msg)

        print("✅ Alert email sent successfully!")

    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# ✅ Schedule Task to Run Every 5 Minutes
schedule.every(5).minutes.do(detect_anomalies)

print("\n📌 Machine Learning-Based Transaction Monitoring Started. Running every 5 minutes...")

# ✅ Keep Script Running
while True:
    schedule.run_pending()
    time.sleep(60)
