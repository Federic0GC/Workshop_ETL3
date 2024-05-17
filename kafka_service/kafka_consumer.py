from kafka import KafkaConsumer
import pandas as pd
import json
import pickle
import os
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
import joblib

# Loading trained model
joblib_file = "model/train_model_forest_regressor.pkl"
loaded_model = joblib.load(joblib_file)
print(loaded_model)

# --- Consumer logic ---
consumer = KafkaConsumer(
    'kafka-workshop-happiness-model',  # Topic to connect to
    bootstrap_servers=['localhost:9092'],  # List of Kafka servers to connect to
    auto_offset_reset='earliest',  
    enable_auto_commit=True,  
    group_id='my-group', 
    value_deserializer=lambda x: x  
)

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")

mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'

db_connection = create_engine(mysql_connection_str)

csv_filename = "happiness_model_prediction.csv"

try:
    for message in consumer:
        print(f"Received row: {message.value}")

        try:
            decoded_message = message.value.decode('utf-8')
            message_data = json.loads(decoded_message)
            
            # Convert message data to DataFrame
            df = pd.DataFrame([message_data])

            # Define the features to use for prediction
            features = ['economy (gdp per capita)', 'family', 'health (life expectancy)', 'freedom', 'trust (government corruption)']
            df['happiness_prediction'] = loaded_model.predict(df[features])

            # Save the data to the MySQL database
            df.to_sql('happiness_prediction_table', con=db_connection, if_exists='append', index=False)
            print("Data successfully saved to the MySQL database.")

            # Save the data to a CSV file
            df.to_csv(csv_filename, mode='a', header=not os.path.exists(csv_filename), index=False)
            print("Data successfully saved to the CSV file.")

        except Exception as e:
            print(f"Error processing or saving the message: {e}")

except Exception as e:
    print(f"Error consuming messages from Kafka: {e}")

finally:
    consumer.close()

    if db_connection:
        db_connection.dispose()
    print("Database connection closed.")
