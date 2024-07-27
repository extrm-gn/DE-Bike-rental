CREATE TABLE IF NOT EXISTS country_table (
    country_id INT PRIMARY KEY,
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
);

CREATE TABLE IF NOT EXISTS city_table (
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
);

CREATE TABLE IF NOT EXISTS city_country_table (
    id SERIAL PRIMARY KEY,
    country_id INT,
    city_id INT,
    mean_temperature NUMERIC(10,6),
    date_gathered DATE,
    FOREIGN KEY(country_id) REFERENCES country_table(country_id),
    FOREIGN KEY(city_id) REFERENCES city_table(city_id)
);