FROM python:3.9

RUN pip install pandas requests python-dotenv psycopg2

WORKDIR /app

COPY Datasets /app/Datasets
COPY .env /app/
COPY city_sampler.py /app/
COPY data_ingestion.py /app/
COPY generate_sql_inserts.py /app/

CMD ["python", "city_sampler.py"]