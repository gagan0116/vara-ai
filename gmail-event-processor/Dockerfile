FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Cloud Run requires listening on $PORT (8080)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
