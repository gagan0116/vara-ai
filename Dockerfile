# Use official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Set working directory
WORKDIR /app

# Install system dependencies (build tools for some python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose the port
EXPOSE 8080

# Inject cross-cloud GCP Credentials (file is copied via COPY . .)
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/gcp_keys.json

# Command to run the application using uvicorn
# --timeout-keep-alive: Keep connections alive for long-running compilation jobs
CMD ["uvicorn", "knowledge_base_server.main:app", "--host", "0.0.0.0", "--port", "8080", "--timeout-keep-alive", "300"]
