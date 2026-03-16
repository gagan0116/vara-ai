FROM python:3.11

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY mcp_processor/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy MCP Processor application
COPY mcp_processor/app.py .
COPY mcp_processor/processor.py .

# Copy MCP Server Modules
COPY doc_server ./doc_server
COPY defect_analyzer ./defect_analyzer
COPY db_verification ./db_verification
COPY policy_compiler_agents ./policy_compiler_agents
COPY neo4j_graph_engine ./neo4j_graph_engine
COPY policy_docs ./policy_docs
COPY bedrock_client.py .

# Set PYTHONPATH to include /app
ENV PYTHONPATH=/app
ENV PORT=8080

# Inject cross-cloud GCP Credentials
COPY gcp_keys.json /app/gcp_keys.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/gcp_keys.json

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
