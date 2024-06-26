import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import time
from kafka import KafkaProducer
from json import dumps

# Data from 2015
data_2015_csv = 'data/2015.csv' 
data_2015 = pd.read_csv(data_2015_csv, delimiter=',') 

# Data from 2016
data_2016_csv = 'data/2016.csv' 
data_2016 = pd.read_csv(data_2016_csv, delimiter=',') 

# Data from 2017
data_2017_csv = 'data/2017.csv' 
data_2017 = pd.read_csv(data_2017_csv, delimiter=',') 

# Data from 2018
data_2018_csv = 'data/2018.csv' 
data_2018 = pd.read_csv(data_2018_csv, delimiter=',') 

# Data from 2019
data_2019_csv = 'data/2019.csv' 
data_2019 = pd.read_csv(data_2019_csv, delimiter=',')

# Normalizing column names
data_2017 = data_2017.rename(columns={
    'Happiness.Rank': 'Happiness Rank',
    'Happiness.Score': 'Happiness Score',
    'Economy..GDP.per.Capita.': 'Economy (GDP per Capita)',
    'Health..Life.Expectancy.': 'Health (Life Expectancy)',
    'Trust..Government.Corruption.': 'Trust (Government Corruption)'
})

data_2018 = data_2018.rename(columns={
    'Overall rank': 'Happiness Rank',
    'Country or region': 'Country',
    'Score': 'Happiness Score',
    'GDP per capita': 'Economy (GDP per Capita)',
    'Freedom to make life choices': 'Freedom',
    'Perceptions of corruption': 'Trust (Government Corruption)',
    'Healthy life expectancy': 'Health (Life Expectancy)',
    'Social support': 'Family'
})

data_2019 = data_2019.rename(columns={
    'Overall rank': 'Happiness Rank',
    'Country or region': 'Country',
    'Score': 'Happiness Score',
    'GDP per capita': 'Economy (GDP per Capita)',
    'Freedom to make life choices': 'Freedom',
    'Perceptions of corruption': 'Trust (Government Corruption)',
    'Healthy life expectancy': 'Health (Life Expectancy)',
    'Social support': 'Family'
})

# Adding year column to each dataframe
data_2015['year'] = 2015
data_2016['year'] = 2016
data_2017['year'] = 2017
data_2018['year'] = 2018
data_2019['year'] = 2019

# Merge
models_dataframes = [data_2015, data_2016, data_2017, data_2018, data_2019]
model_dataset = pd.concat(models_dataframes, ignore_index=True)

# Normalizing columns
model_dataset.columns = model_dataset.columns.str.lower()

numeric_columns = model_dataset.select_dtypes(include=['float64', 'int64']).columns
model_dataset[numeric_columns] = model_dataset[numeric_columns].fillna(model_dataset[numeric_columns].mean())

selected_fields = ['economy (gdp per capita)', 'family', 'health (life expectancy)', 'freedom', 
                   'trust (government corruption)', 'happiness score']

# Dataframe with selected fields
train_model = model_dataset[selected_fields].copy()

X = train_model.drop("happiness score", axis=1)
y = train_model["happiness score"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Dataframe for test data
test_data = pd.DataFrame(X_test, columns=X.columns)

# Adding the target variable (y_test) to the test data DataFrame
test_data["happiness_score"] = y_test.values

# -------- Producer logic ---------------
producer = KafkaProducer(
   value_serializer=lambda m: dumps(m).encode('utf-8'),
   bootstrap_servers = ['localhost:9092'])

for i in range(len(test_data)):
    test_iloc = test_data.iloc[i]
    test_dict = test_iloc.to_dict()
    producer.send('kafka-workshop-happiness-model', value=test_dict)
    time.sleep(1)
