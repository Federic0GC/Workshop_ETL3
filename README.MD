
# Workshop_ETL 3

## Context

## Workshop 3: ETL Process and Happiness Prediction with Kafka Service

### The project focuses on cleaning 5 datasets corresponding to different dates but sharing various common fields among them. The idea is to perform proper cleaning of these 5 datasets to achieve a merge where we leverage the characteristics provided separately by each dataset. Additionally, we can add one more dataset corresponding to the dates, enabling us to divide the records of each dataset by their dates for identification after the merge.

### After cleaning and the corresponding merge, a "feature selection" process is carried out, with which the model will be trained to predict the happiness score. The model will not be trained with all the features from the merge, as there are fields within it that are not related to the happiness score. In the case of this project, feature selection was performed using a correlation matrix with the field to be predicted. This allows us to choose the features most directly related to the value to be predicted.

### Once the features are selected, the model is trained to predict the happiness score using the most appropriate fields chosen for this task. This training was done with a random forest regressor model in search of the most accurate R2 score. Now, after this process, the aim is to save the model as a joblib file in our model folder. Later, we will call it in our Kafka consumer to predict each incoming data in real-time.

### The Kafka process is straightforward. Here, the goal is to send our feature selection from a Kafka producer, specifically the "test" data section, which in our case constitutes 20% of our dataset. These data will be sent in the form of a dictionary, record by record, to a Kafka consumer. As each data point arrives, the consumer will simultaneously call the pre-trained model to obtain the happiness prediction score for each incoming record, storing it in real-time in a database.

### To conclude, we will call the table stored in our database to extract the R2 score and the margin of error between the 'happiness_score' field and the field predicted by the model, named 'happiness_prediction'. You can find this process in the Python file located in the 'score_model' folder.

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

### Make sure to have a .env file in your project which will define your credentials for connecting to your MySQL database, your .env file should look like this:

### DB_USER=your_username
### DB_PASSWORD=your_password
### DB_HOST=your_host
### DB_DATABASE=your_database



## ---Kafka Installation---

## 4. Running the Kafka container 

###  Make sure to have Docker installed and run this command with the docker-compose.yml file open in a terminal
```docker compose up```

### Access the Kafka container
``` docker exec -it kafka bash ```

### Once inside the container, we will create a topic to which our Kafka producer and Kafka consumer will connect to send and receive data
``` kafka-topics --bootstrap-server kafka --create --topic kafka-workshop-happiness-model ```

## ---Kafka Stream---

### Make sure to open two terminals, one for the consumer and the other for the producer. You'll define this with the following command, where each terminal will execute one of the two Python files. In the first terminal, run the feature_selection_producer.py file, and in the second terminal, run the kafka_consumer.py file.

``` python feature_selection_producer.py.py ```

``` python kafka_consumer.py ```




