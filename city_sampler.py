import pandas as pd

df = pd.read_csv("Datasets/worldcities.csv")
country_unique = list(set(df.country.to_list()))


print(country_unique)