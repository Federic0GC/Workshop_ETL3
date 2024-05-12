from kafka import KafkaConsumer
import pandas as pd
import json
import pickle
import os
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
import joblib

# Cargando modelo entrenado
joblib_file = "model/train_model_forest_regressor.pkl"
loaded_model = joblib.load(joblib_file)
print(loaded_model)
# --- Logica consumer ---
consumer = KafkaConsumer(
    'kafka_lab2',  # Tema al cual se va a conectar
    bootstrap_servers=['localhost:9092'],  # Lista de servidor kafka al que se va a conectar
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
        print(f"Fila recibida: {message.value}")

        try:
            decoded_message = message.value.decode('utf-8')
            message_data = json.loads(decoded_message)
            
           
            df = pd.DataFrame([message_data])

            
            features = ['economy (gdp per capita)', 'family', 'health (life expectancy)', 'freedom', 'trust (government corruption)']
            df['happiness_prediction'] = loaded_model.predict(df[features])

            
            df.to_sql('happiness_prediction_table', con=db_connection, if_exists='append', index=False)
            print("Datos guardados correctamente en la base de datos MySQL.")

           
            df.to_csv(csv_filename, mode='a', header=not os.path.exists(csv_filename), index=False)
            print("Datos guardados correctamente en el archivo CSV.")

        except Exception as e:
            print(f"Error al procesar o guardar el mensaje: {e}")

except Exception as e:
    print(f"Error al consumir mensajes de Kafka: {e}")

finally:
    consumer.close()

    if db_connection:
        db_connection.dispose()
    print("Conexión a la base de datos cerrada.")
