# Python AirFlow Combined Assignment Questions:

Subscribe to https://rapidapi.com/community/api/open-weather-map/

Use the docker container to create airflow and postgres instances.

## Create the first task to use endpoint /Current Weather Data for at least 10 states of India and fill up the csv file with details of State, Description, Temperature, Feels Like Temperature, Min Temperature, Max Temperature, Humidity, Clouds.

Ans: ![image](https://user-images.githubusercontent.com/47415702/158744147-554ba3f9-dfc5-4bc6-987c-ef861a449ef7.png)

## Create a second task to create a postgres table “Weather” that would have columns same as the csv file.

Ans: ![image](https://user-images.githubusercontent.com/47415702/158744176-20bf9a22-df3b-496a-a1c4-5b02dbc70556.png)

## Create a third task that should fill the columns of the table while reading the data from the csv file.

Ans: ![image](https://user-images.githubusercontent.com/47415702/158744221-671ac271-8368-4d8e-ac2c-9b6e13e4d4aa.png)

## Schedule the DAG in AirFlow to run every day at 6:00 am and update the daily weather detail in csv as well as the table.

Ans: ![image](https://user-images.githubusercontent.com/47415702/158744267-6e5831d7-5e92-47eb-90aa-8b163c10dadf.png)

