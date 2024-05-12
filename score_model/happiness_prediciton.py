import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from sklearn.metrics import mean_squared_error, r2_score

load_dotenv()


db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_database = os.getenv("DB_DATABASE")

mysql_connection_str = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}'


db_connection = create_engine(mysql_connection_str)

try:
    df = pd.read_sql('SELECT * FROM happiness_prediction_table', con=db_connection)
    mse = mean_squared_error(df['happiness_score'], df['happiness_prediction'])

    r2 = r2_score(df['happiness_score'], df['happiness_prediction'])

    print(f"Error Cuadrático Medio (MSE): {mse}")
    print(f"Coeficiente de Determinación (R2 Score): {r2}")

except Exception as e:
    print(f"Error al consultar la base de datos MySQL: {e}")

finally:
    if db_connection:
        db_connection.dispose()
    print("Conexión a la base de datos cerrada.")
