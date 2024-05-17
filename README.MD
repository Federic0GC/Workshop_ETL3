
# Workshop_ETL 3

## Context

## Workshop 3: Proceso de ETL y prediccion de felicidad con servicio de kafka
### El proyecto se centra en la limpieza de 5 datasets que corresponden a diferentes fechas pero que tienen en comun entre si diferentes campos, la idea es hacer la correcta limpieza de estos 5 datasets para lograr un merge en el cual se aproveche las caracteristicas que todos los datasets por separado nos dio, pudiendo añadir uno mas el cual corresponda a las fechas, pudiendo dividir los registros de cada dataset por sus fechas para identificarlas despues del merge.

### Despues de la limpieza y el correspondiente merge, se realiza un proceso de "seleccion de caracteristicas" con los cuales el modelo va a ser entrenado para predecir el puntaje de felicidad (el modelo no va a ser entrenado con todas las caracteristicas del merge, ya que en este mismo encontramos campos que no estan relacionados con el puntaje de felicidad). en el caso de este proyecto se realizo la seleccion de caracteristicas por medio de una matriz de correlacion con el campo a predecir, para asi poder elegir las caracteristicas mas relacionadas directamente con el valor a predecir.

### Ya seleccionado las caracteristicas se realiza un entrenamiento del modelo que pueda predecir el puntaje de felicidad con los campos que seleccionamos mas correctos para ese trabajo, este entrenamiento se hizo con un modelo de regresor de bosque aleatorio en busqueda del r2 score mas acertado. Ahora bien despues de este proceso se busca guardar el modelo como un archivo joblib en nuestra carpeta model, el cual llamaremos despues en nuestro kafka consumer para predecir cada dato que le este llegando en tiempo real

### El proceso de kafka es sencillo, en este se busca enviar desde un kafka producer  nuestro feature selection, solo el apartado de datos de "test" que en nuestro caso es el 20% de nuestro dataset, estos datos se enviaran en forma de diccionario registro a registro a un kafka consumer que al mismo tiempo que le llega cada dato, llamara al modelo ya entrenado en busqueda de que nos de el score de prediccion de felicidad para cada registro que le este llegando, guardandolo en tiempo real en una base de datos

### Ya para finalizar llamaremos a la tabla guardada en nuestra base de datos para poder sacar el r2 score y el margen de error entre el campo de happiness_score y el campo que el modelo predijo llamado happiness_prediction, este proceso lo podras encontrar el archivo python que esta en la carpeta score_model

- Python
- Jupyter notebook
- CSV files
- MySQL Workbench
- Kafka
- Docker


## Steps to use and clone this repository

## 1. Clone this repository to your system

```git clone https://github.com/Federic0GC/Workshop_ETL3.git```

## 2. Install the requirements in your virtual environment from "requirements.txt"
``` pip install -r requirements.txt```

## 3. Make sure to install MySQL Workbench and Python on your system

## ---Kafka Installation---

## 4. Correr el contenedor de kafka con airflow

###  Asegura tener instalado docker y iniciar este comando teniendo el archivo de docker-compose.yml abierto con una terminal
```docker compose up```

### Accede al contenedor de kafka
``` docker exec -it kafka bash ```

### Ya dentro del contenedor vamos a crear un topic al cual se conectara nuestro kafka producer y kafka consumer para enviar y recibir los datos
``` kafka-topics --bootstrap-server kafka --create --topic kafka-workshop-happiness-model``

## ---Kafka Stream---

### Asegurate de abrir dos terminales en las cuales una va a ser el consumidor y otro el productor, eso lo definiras con el siguiente comando, en el cual en cada terminal vas a llamar que ejecute uno de los dos archivos python, en la primer terminal el archivo python de feature_selection_producer.py y en la segunda terminal el archivo kafka_consumer.py

``` python feature_selection_producer.py.py ```

``` python kafka_consumer.py ```




