import csv
import requests
from datetime import datetime


def data_processing():
    api_url = "https://community-open-weather-map.p.rapidapi.com/weather"
    api_key = "0ca641d880msh4cb905886943e03p186064jsnb7cafc1fce4e"

    # creating the headers of Rapid API
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
    }

    # List of all 10 cities
    city = ["Patna", "Delhi", "Goa", "Bangalore", "Hyderabad", "Jaipur", "Punjab", "Lucknow", "Srinagar", "Kolkata"]
    header = ['City-Name', 'Description', 'Temperature', 'Feels Like Temperature', 'Min Temperature', 'Max Temperature',
              'Humidity', 'Clouds']

    # Creating the list of cities to store the data in csv file
    li = []

    # Looping over all the cities
    for i in city:
        # Using error handling to call the Rapid API to catch the error
        try:
            query = {"q": f"{i},india", "lat": "0", "lon": "0", "id": "2172797", "lang": "null",
                     "units": "imperial", "mode": "JSON"}
            response = requests.get(api_url, headers=headers, params=query)
            data = response.json()
            data_city = [data['name'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'],
                    data['main']['temp_min'],
                    data['main']['temp_max'], data['main']['humidity'], data['clouds']['all']]

            li.append(data_city)  # Appending all the weather data into the list
            print(data_city)
        except:
            print("Request Denied")

    # Writing into the csv file
    try:
        with open('/usr/local/airflow/store_files_airflow/weather_data.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(li)

    except:
        print("Cannot write in csv")



