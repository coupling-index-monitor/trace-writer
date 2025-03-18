from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.trace_writer import run_trace_writer

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: ")
    run_trace_writer()
    
    yield 

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
