import pandas
import psycopg2

hostname = 'localhost'
db = 'Weather'
username = 'postgres'
pwd = '17JE003089'
port_id = 5432


def create_table_weather_data():
    df = pandas.read_csv("Weather_Data.csv")
    print(df)

    create_table = """CREATE TABLE weather(
        STATE VARCHAR(30),
        DESCRIPTION varchar(30),
        TEMPERATURE decimal,
        FEELS_LIKE_TEMPERATURE decimal,
        MIN_TEMP decimal,
        MAX_TEMP decimal,
        HUMIDITY numeric,
        CLOUDS numeric)"""

    try:
        conn = psycopg2.connect(
            user=username,
            host=hostname,
            database=db,
            password=pwd
        )
        cursor = conn.cursor()

        cursor.execute(create_table)

        print("Table Created successfully")


    except:
        print("Error in connection")
    finally:
        conn.close()
        print("No issues")
