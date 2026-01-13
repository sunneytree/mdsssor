"""Lambda URL polling manager

Manages round-robin polling of multiple Lambda endpoints for video generation.
"""
import asyncio
import httpx
from typing import List, Optional, Dict, Any
from ..core.database import Database
from ..core.models import LambdaConfig


class LambdaManager:
    """Manages Lambda URL polling and load balancing"""
    
    def __init__(self):
        self.db = Database()
        self._current_index = 0
        self._lock = asyncio.Lock()
        self._config_cache: Optional[List[LambdaConfig]] = None
        self._cache_time = 0
        self._cache_ttl = 60  # Cache config for 60 seconds
    
    async def _get_config(self) -> List[LambdaConfig]:
        """Get Lambda configuration with caching"""
        import time
        current_time = time.time()
        
        if (self._config_cache is None or 
            current_time - self._cache_time > self._cache_ttl):
            self._config_cache = await self.db.get_lambda_configs()
            self._cache_time = current_time
        
        return self._config_cache
    
    def _get_urls(self, configs: List[LambdaConfig]) -> List[str]:
        """Get enabled Lambda URLs from per-row configurations"""
        urls = []
        for cfg in configs:
            if not cfg.lambda_enabled:
                continue
            if not cfg.lambda_api_url:
                continue
            urls.append(cfg.lambda_api_url.strip())
        return urls

    def _get_endpoints(self, configs: List[LambdaConfig]) -> List[Dict[str, str]]:
        """Build endpoint list from per-row configurations"""
        endpoints = []
        for cfg in configs:
            if not cfg.lambda_enabled:
                continue
            if not cfg.lambda_api_url:
                continue
            api_key = cfg.lambda_api_key or ""
            if not api_key:
                continue
            endpoints.append({
                "url": cfg.lambda_api_url.strip(),
                "key": api_key
            })
        return endpoints
    
    async def get_next_endpoint(self) -> Optional[Dict[str, str]]:
        """Get next endpoint using round-robin polling"""
        configs = await self._get_config()
        endpoints = self._get_endpoints(configs)
        if not endpoints:
            return None

        async with self._lock:
            # Round-robin selection
            endpoint = endpoints[self._current_index % len(endpoints)]
            self._current_index = (self._current_index + 1) % len(endpoints)
            return endpoint
    
    async def get_api_key(self) -> Optional[str]:
        """Get a Lambda API key from the first available endpoint"""
        configs = await self._get_config()
        endpoints = self._get_endpoints(configs)
        return endpoints[0]["key"] if endpoints else None
    
    async def is_enabled(self) -> bool:
        """Check if Lambda is enabled"""
        configs = await self._get_config()
        return any(cfg.lambda_enabled for cfg in configs)
    
    async def get_all_urls(self) -> List[str]:
        """Get all configured URLs"""
        configs = await self._get_config()
        return self._get_urls(configs)

    async def has_available_endpoints(self) -> bool:
        """Check if there is any usable endpoint"""
        configs = await self._get_config()
        return bool(self._get_endpoints(configs))
    
    async def create_task(self, token: str, payload: Dict[str, Any]) -> str:
        """Create task using next available Lambda endpoint
        
        Args:
            token: Access token
            payload: Task creation payload
            
        Returns:
            Task ID from Lambda response
            
        Raises:
            HTTPException: If all endpoints fail or Lambda is disabled
        """
        from fastapi import HTTPException
        
        if not await self.is_enabled():
            raise HTTPException(status_code=400, detail="Lambda is not enabled")
        
        configs = await self._get_config()
        urls = self._get_urls(configs)
        if not urls:
            raise HTTPException(status_code=400, detail="No Lambda URLs configured")

        endpoints = self._get_endpoints(configs)
        if not endpoints:
            raise HTTPException(status_code=400, detail="Lambda API key not configured")
        
        # Try each URL in round-robin order
        last_error = None
        for attempt in range(len(endpoints)):
            endpoint = await self.get_next_endpoint()
            if not endpoint:
                continue
            
            try:
                task_id = await self._post_create_task(endpoint["url"], endpoint["key"], token, payload)
                print(f"✅ [Lambda] Task created successfully using {endpoint['url']}: {task_id}")
                return task_id
            except Exception as e:
                last_error = e
                print(f"⚠️ [Lambda] Failed to create task using {endpoint['url']}: {str(e)}")
                continue
        
        # All endpoints failed
        error_msg = f"All Lambda endpoints failed. Last error: {str(last_error)}"
        print(f"❌ [Lambda] {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    
    async def _post_create_task(self, lambda_url: str, api_key: str, 
                               token: str, payload: Dict[str, Any]) -> str:
        """Post task creation request to specific Lambda endpoint
        
        Args:
            lambda_url: Lambda endpoint URL
            api_key: Lambda API key
            token: Access token
            payload: Task creation payload
            
        Returns:
            Task ID from response
            
        Raises:
            Exception: If request fails or response is invalid
        """
        headers = {
            "Content-Type": "application/json",
            "x-lambda-key": api_key
        }
        
        request_data = {
            "token": token,
            "payload": payload
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                lambda_url,
                json=request_data,
                headers=headers
            )
        
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
        
        try:
            data = response.json()
        except Exception:
            raise Exception("Invalid JSON response")
        
        task_id = data.get("id") or data.get("task_id")
        if not task_id:
            raise Exception("No task ID in response")
        
        return task_id
    
    def invalidate_cache(self):
        """Invalidate configuration cache"""
        self._config_cache = None
        self._cache_time = 0


# Global instance
lambda_manager = LambdaManager()
