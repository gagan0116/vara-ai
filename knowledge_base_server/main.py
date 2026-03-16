# mcp_server/main.py
import os
from typing import List
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .compiler_service import create_compilation_job, get_job_status
from policy_compiler_agents.visualize_graph import fetch_graph_data

app = FastAPI(title="Policy Compiler Agent")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],

)

# API Routes
@app.post("/api/compile")
async def compile_policy(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...)
):
    """
    Upload PDF files and start the compilation pipeline.
    Returns a job_id for polling.
    """
    api_key = os.getenv("LLAMA_CLOUD_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="LLAMA_CLOUD_API_KEY not configured")
        
    job_id = await create_compilation_job(files, api_key)
    return {"job_id": job_id, "message": "Compilation started", "status": "processing_upload"}

@app.get("/api/status/{job_id}")
async def check_status(job_id: str):
    """Check the status of a compilation job."""
    status = get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status

@app.get("/api/graph")
async def get_graph():
    """Fetch the current knowledge graph data for visualization."""
    try:
        data = await fetch_graph_data()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "ok"}

# Mount static files (Frontend)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
