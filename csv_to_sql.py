from sqlalchemy import create_engine, text
import pandas as pd


def table_schema():

    # SQL command to create country dimension
    country_dimension = text('''CREATE TABLE IF NOT EXISTS country_table
                           (country_id INT PRIMARY KEY,
                            country_name VARCHAR(50),
                            country_region VARCHAR(30),
                            country_population INT,
                            country_population_density INT,
                            country_GDP INT,
                            country_GDP_per_capita NUMERIC(15,2),
                            country_surface_area INT,
                            country_sex_ratio NUMERIC(5,2),
                            activation_date DATE,
                            expiration_date DATE,
                            status VARCHAR(10)
                            )''')

    # SQL command to create city dimension
    city_dimension = text('''CREATE TABLE IF NOT EXISTS city_table (
                        city_id INT PRIMARY KEY,
                        city_name VARCHAR(50),
                        city_latitude NUMERIC(10,6),
                        city_longitude NUMERIC(10,6),
                        city_ISO3 VARCHAR(5),
                        city_population INT,
                        city_capital VARCHAR(15),
                        activation_date DATE,
                        expiration_date DATE,
                        status VARCHAR(10)
                        )''')

    # SQL command to create city and country fact table
    city_country_fact = text('''CREATE TABLE IF NOT EXISTS city_country_table (
                           id SERIAL PRIMARY KEY,
                           country_id INT,
                           city_id INT,
                           mean_temperature NUMERIC(10,6),
                           FOREIGN KEY(country_id) REFERENCES country_table(country_id),
                           FOREIGN KEY(city_id) REFERENCES city_table(city_id) )''')

    return country_dimension, city_dimension, city_country_fact


def create_table():
    DB_URL = 'postgresql://root:root@localhost:5432/bike_db'
    engine = create_engine(DB_URL)

    country_dimension, city_dimension, city_country_fact = table_schema()

    with engine.connect() as connection:
        connection.execute(country_dimension)
        connection.execute(city_dimension)
        connection.execute(city_country_fact)

        df = pd.read_csv('Datasets/selected_city.csv')

        df.to_sql(name="please_table", con=connection, if_exists='append')

        print("done here")


def main():
    create_table()
    print("Tables created successfully")


if __name__ == '__main__':
    main()