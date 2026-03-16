# mcp_server/compiler_service.py
import asyncio
import os
import uuid
import shutil
import logging
from typing import Dict, Any, List
from datetime import datetime

from policy_compiler_agents.ingestion import parse_documents
from policy_compiler_agents.agent import PolicyCompilerPipeline
from policy_compiler_agents.tools import PROJECT_ROOT

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory job store (deployment note: use Redis/DB for production)
jobs: Dict[str, Dict[str, Any]] = {}

class CompilerJob:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.status = "created" # created, processing_upload, ingestion, compiling, completed, failed
        self.created_at = datetime.now().isoformat()
        self.logs = []
        self.result = None
        self.error = None
        self.progress = 0

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "status": self.status,
            "created_at": self.created_at,
            "logs": self.logs[-50:], # Return last 50 logs for detailed progress
            "progress": self.progress,
            "error": self.error,
            "result": self.result
        }

    def log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] {message}"
        self.logs.append(entry)
        logger.info(f"Job {self.job_id}: {message}")

async def create_compilation_job(files: List[Any], api_key: str) -> str:
    """Creates a job and starts processing."""
    job_id = str(uuid.uuid4())
    job = CompilerJob(job_id)
    jobs[job_id] = job
    
    # Start background processing
    asyncio.create_task(process_compilation(job_id, files, api_key))
    
    return job_id

async def process_compilation(job_id: str, files: List[Any], api_key: str):
    """Background task to run the full pipeline."""
    job = jobs.get(job_id)
    if not job:
        return

    try:
        job.status = "processing_upload"
        job.log("Starting job processing...")
        job.progress = 5

        # 1. Save uploaded files to temp directory
        temp_dir = os.path.join(PROJECT_ROOT, "policy_docs", "policy_pdfs", job_id)
        os.makedirs(temp_dir, exist_ok=True)
        
        job.log(f"Saving {len(files)} files to {temp_dir}...")
        for file in files:
            file_path = os.path.join(temp_dir, file.filename)
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)
        
        # 2. Ingestion (LlamaParse)
        job.status = "ingestion"
        job.progress = 10
        job.log("Starting LlamaParse ingestion...")
        
        output_file = os.path.join(PROJECT_ROOT, "policy_docs", "combined_policy.md")
        
        ingestion_result = await parse_documents(temp_dir, output_file, api_key)
        
        if ingestion_result["status"] != "success":
            raise Exception(f"Ingestion failed: {ingestion_result.get('message')}")
            
        job.log(f"Ingestion complete. Processed {ingestion_result['files_processed']} files.")
        job.progress = 30
        
        # 3. Policy Compilation Pipeline
        job.status = "compiling"
        job.log("Initializing Policy Compiler Agents...")
        
        # Create a log callback that updates job progress
        def pipeline_logger(message: str):
            job.log(message)
            # Update progress based on stage
            if "[STAGE 1/4]" in message:
                job.progress = 35
            elif "[STAGE 2/4]" in message:
                job.progress = 50
            elif "[STAGE 3/4]" in message:
                job.progress = 70
            elif "[STAGE 4/4]" in message:
                job.progress = 85
            elif "COMPLETE" in message:
                job.progress = 95
        
        pipeline = PolicyCompilerPipeline(log_callback=pipeline_logger)
        
        job.log("Running Ontology, Extraction, Critic, and Builder agents...")
        
        # Clear existing graph for clean slate
        pipeline_result = await pipeline.run(clear_existing_graph=True)
        
        if pipeline_result["pipeline_status"] != "success":
             raise Exception(f"Pipeline failed: {pipeline_result}")

        job.progress = 100
        job.status = "completed"
        job.result = pipeline_result["final_state"]
        job.log("Knowledge Graph construction complete!")
        
        # Cleanup temp files
        try:
            shutil.rmtree(temp_dir)
        except:
            pass

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.log(f"ERROR: {str(e)}")
        logger.exception("Job failed")

def get_job_status(job_id: str) -> Dict[str, Any]:
    job = jobs.get(job_id)
    if not job:
        return None
    return job.to_dict()
