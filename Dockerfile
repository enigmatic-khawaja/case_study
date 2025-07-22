# Use Python 3.9 with Jupyter and Spark
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV JUPYTER_PORT=8888
ENV JUPYTER_TOKEN=etlpipeline

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unixodbc-dev \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies in two stages for better caching
RUN pip install --no-cache-dir \
    jupyterlab==3.6.3 \
    pyspark==3.4.1 \
    findspark && \
    pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY src/ ./src/
COPY notebooks/ ./notebooks/
COPY config.yaml .
COPY data/ ./data/
COPY sql/ ./sql/
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Create directories
RUN mkdir -p logs

# Simple startup command
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=etlpipeline", "--notebook-dir=/app/notebooks"]