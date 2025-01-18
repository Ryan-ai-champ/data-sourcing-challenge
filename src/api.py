from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
from .async_client import AsyncAPIClient
from .monitoring import monitor_request, start_metrics_server

app = FastAPI(
    title="NASA Space Weather API",
    description="API for retrieving and analyzing space weather data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DataRequest(BaseModel):
    start_date: str
    end_date: str
    data_type: str

class DataResponse(BaseModel):
    data: List[dict]
    metadata: dict

@app.on_event("startup")
async def startup_event():
    start_metrics_server()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/api/v1/space-weather", response_model=DataResponse)
@monitor_request(endpoint="/api/v1/space-weather", method="POST")
async def get_space_weather_data(request: DataRequest):
    async with AsyncAPIClient(base_url="https://api.nasa.gov") as client:
        try:
            data = await client.get(
                f"/DONKI/{request.data_type}",
                params={
                    "startDate": request.start_date,
                    "endDate": request.end_date
                }
            )
            return DataResponse(
                data=data,
                metadata={
                    "request_params": request.dict(),
                    "count": len(data)
                }
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

