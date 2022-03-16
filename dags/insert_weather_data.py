import pandas
import psycopg2

hostname = 'localhost'
db = 'Weather'
username = 'postgres'
pwd = '17JE003089'
port_id = 5432


def load_weather_data():
    df = pandas.read_csv("Weather_Data.csv")
    print(df)

    try:
        conn = psycopg2.connect(
            user=username,
            host=hostname,
            database=db,
            password=pwd
        )
        cursor = conn.cursor()

        insert_query = "Insert into weather (STATE, DESCRIPTION, TEMPERATURE, FEELS_LIKE_TEMPERATURE,MIN_TEMP, " \
                       "MAX_TEMP,HUMIDITY,CLOUDS) values (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (STATE) " \
                       "UPDATE SET DESCRIPTION = excluded.DESCRIPTION, TEMPERATURE = excluded.TEMPERATURE, " \
                       "FEELS_LIKE_TEMPERATURE = excluded.FEELS_LIKE_TEMPERATURE, MIN_TEMP = excluded.MIN_TEMP, " \
                       "MAX_TEMP = excluded.MAX_TEMP, HUMIDITY = excluded.HUMIDITY, CLOUDS = excluded.CLOUDS;" \

        for index, row in df.iterrows():
            print("added..")
            cursor.execute(insert_query, (
                row['State'], row['Description'], row['Temperature'], row['Feels_Like_Temperature']
                , row['Min_Temperature'], row['Max_Temperature'], row['Humidity'], row['Clouds']))

        conn.commit()

        print("Success 200")

    except:
        print("Failed to connect")
    finally:
        conn.close()
