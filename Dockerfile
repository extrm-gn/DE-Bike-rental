FROM python:3.9

RUN pip install pandas requests python-dotenv

WORKDIR /app

# Copy necessary files
COPY Datasets /app/Datasets
COPY .env /app/
COPY city_sampler.py /app/
COPY data_ingestion.py /app/

CMD ["python", "city_sampler.py"]
