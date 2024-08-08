FROM python:3.9

RUN pip install pandas requests python-dotenv psycopg2 dagster dagster-webserver dagit dagster_postgres

WORKDIR /app
ENV DAGSTER_HOME=/app

COPY dagster.yaml .
COPY workspace.yaml .
COPY Datasets /app/Datasets
COPY .env /app/
COPY city_sampler.py /app/
COPY data_ingestion.py /app/
COPY generate_sql_inserts.py /app/
COPY dags/repository.py /app/dags/repository.py

EXPOSE 3000

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]