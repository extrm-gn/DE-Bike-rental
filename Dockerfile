FROM python:3.9

RUN pip install pandas requests python-dotenv psycopg2

WORKDIR /app

# Copy necessary files
COPY Datasets /app/Datasets
COPY .env /app/
COPY city_sampler.py /app/
COPY data_ingestion.py /app/
COPY generate_sql_inserts.py /app/

# Set entrypoint script
#COPY entrypoint.sh /app/
#RUN chmod +x /app/entrypoint.sh

# Command to run the entrypoint script
#CMD ["/app/entrypoint.sh"]
CMD ["python", "city_sampler.py"]