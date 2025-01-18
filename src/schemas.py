from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, validator

class CMEData(BaseModel):
    start_time: datetime
    end_time: Optional[datetime]
    speed: float = Field(ge=0)
    type: str
    angle: Optional[float] = Field(None, ge=0, le=360)
    
    @validator('type')
    def validate_type(cls, v):
        valid_types = {'CME', 'partial_halo', 'halo'}
        if v.lower() not in valid_types:
            raise ValueError(f'Type must be one of {valid_types}')
        return v.lower()

class GSTData(BaseModel):
    time: datetime
    kp_index: float = Field(ge=0, le=9)
    dst_index: float
    
    @validator('kp_index')
    def validate_kp_index(cls, v):
        if not isinstance(v, (int, float)):
            raise ValueError('Kp index must be a number')
        return float(v)

class DataResponse(BaseModel):
    status: str
    data: List[dict]
    timestamp: datetime = Field(default_factory=datetime.utcnow)

