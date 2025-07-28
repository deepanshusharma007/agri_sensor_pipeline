# 1. Use the official Python image as base
FROM python:3.12-slim

# 2. Set working directory to /app
WORKDIR /app

# 3. Copy everything in your repo into the Docker container’s /app folder
COPY . /app

# 4. Install required Python dependencies
RUN pip install --upgrade pip && \
    pip install pandas duckdb pyarrow numpy python-snappy pytest

# 5. By default start shell (or you could RUN your scripts automatically if desired)
ENTRYPOINT ["bash"]
