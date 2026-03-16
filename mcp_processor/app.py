from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from processor import MCPProcessor
from sse_starlette.sse import EventSourceResponse
import os
import json

# Initialize Processor
processor = MCPProcessor()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to MCP servers
    print("🚀 Initializing MCP Processor & Connecting to Servers...")
    try:
        await processor.connect_to_all_servers()
        print("✅ MCP Servers Connected")
    except Exception as e:
        print(f"❌ Failed to connect to MCP servers at startup: {e}")
        # We might want to let it fail so Cloud Run restarts it
        # But let's allow startup so we can see logs
    
    yield
    
    # Shutdown
    print("🛑 Shutting down MCP Processor...")
    await processor.cleanup()

app = FastAPI(lifespan=lifespan)

# CORS - Allow requests from GCS-hosted frontend and localhost for dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://vara-483300.web.app",
        "https://vara-ai.com",
        "https://staging.d1vug68j94viep.amplifyapp.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/process")
async def process_task(request: Request):
    """
    Endpoint triggered by Cloud Tasks.
    Expected Payload: { "bucket": "...", "blob_path": "..." }
    """
    try:
        data = await request.json()
        print(f"📥 Received Task: {data}")
        
        bucket = data.get("bucket")
        blob_path = data.get("blob_path")
        
        if not bucket or not blob_path:
            print("⚠️ Missing bucket or blob_path")
            return {"status": "ignored", "reason": "missing args"}
            
        # Process the email
        await processor.process_single_email(bucket, blob_path)
        
        return {"status": "success"}
        
    except Exception as e:
        print(f"❌ Error processing task: {e}")
        import traceback
        traceback.print_exc()
        # Return 500 to trigger Cloud Tasks retry
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-demo")
async def process_demo(request: Request):
    """
    Demo endpoint for UI - accepts scenario JSON directly.
    Returns Server-Sent Events for real-time progress updates.
    
    This is a NEW endpoint that doesn't affect existing Cloud Tasks flow.
    """
    try:
        scenario_data = await request.json()
        print(f"📥 Demo Request: {scenario_data.get('category', 'UNKNOWN')} from {scenario_data.get('user_id', 'N/A')}")
        
        async def event_generator():
            try:
                async for event in processor.process_demo_scenario(scenario_data):
                    yield {
                        "event": "progress",
                        "data": json.dumps(event)
                    }
                # Send completion event
                yield {
                    "event": "complete",
                    "data": json.dumps({"status": "complete"})
                }
            except Exception as e:
                import traceback
                traceback.print_exc()
                yield {
                    "event": "error",
                    "data": json.dumps({"error": str(e)})
                }
        
        return EventSourceResponse(event_generator())
        
    except Exception as e:
        print(f"❌ Error in demo endpoint: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

