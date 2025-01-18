import aiohttp
import asyncio
from typing import Dict, Any, Optional
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential

class AsyncAPIClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client not initialized. Use as async context manager")
        
        async with self._session.get(f"{self.base_url}{endpoint}", params=params) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self._session:
            raise RuntimeError("Client not initialized. Use as async context manager")
        
        async with self._session.post(f"{self.base_url}{endpoint}", json=data) as response:
            response.raise_for_status()
            return await response.json()

