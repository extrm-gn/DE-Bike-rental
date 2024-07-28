set -e

# Run city_sampler.py
echo "Running city_sampler.py..."
python city_sampler.py

# Run data_ingestion.py
echo "Running data_ingestion.py..."
python data_ingestion.py

# Run generate_sql_inserts.py
echo "Running generate_sql_inserts.py..."
python generate_sql_inserts.py

# Keep the container running for debugging purposes
tail -f /dev/null