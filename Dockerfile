# Dockerfile

# 1. Base Image: Choose the official Airflow version that matches your environment
# Check your Airflow UI (it shows Version: 2.7.2)
FROM apache/airflow:2.7.2-python3.11 

# 2. Install Required Python Packages
# We install all necessary data science and cloud provider libraries in a single command.
# The --no-cache-dir flag keeps the image size smaller.
RUN pip install --no-cache-dir \
    # Data Science Libraries
	numpy==1.24.3 \
    pandas==2.1.1 \
	pyarrow \
	pandas-gbq \
	apache-airflow-providers-google \
	lifetimes \
    pendulum




# 3. Set the Airflow user (Good practice, often optional if base image handles it)
USER airflow