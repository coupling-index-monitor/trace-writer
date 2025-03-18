from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.trace_writer import run_trace_writer
from apscheduler.schedulers.background import BackgroundScheduler



@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    print("Starting up: ")
    if not scheduler.running:
        scheduler.add_job(run_trace_writer, 'interval', seconds=60, id='cron_run_trace_writer')
        scheduler.start()
        print("Cron job to write traces started.")
    else:
        print("Cron job to write traces already running.")
    
    yield 

    if scheduler.running:
        scheduler.remove_job('cron_run_trace_writer')
        scheduler.shutdown()
        print("Cron job to write traces stopped.")
    else:
        print("Cron job to write traces already stopped.")
    print("Shutting down: ")

app = FastAPI(title="Trace Writer Server", version="0.1.0", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def root():
    return {"message": "Trace Writer Server is running"}