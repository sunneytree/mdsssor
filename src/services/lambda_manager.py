"""Lambda URL polling manager

Manages round-robin polling of multiple Lambda endpoints for video generation.
"""
import json
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
        self._config_cache: Optional[LambdaConfig] = None
        self._cache_time = 0
        self._cache_ttl = 60  # Cache config for 60 seconds
    
    async def _get_config(self) -> LambdaConfig:
        """Get Lambda configuration with caching"""
        import time
        current_time = time.time()
        
        if (self._config_cache is None or 
            current_time - self._cache_time > self._cache_ttl):
            self._config_cache = await self.db.get_lambda_config()
            self._cache_time = current_time
        
        return self._config_cache
    
    def _parse_urls(self, config: LambdaConfig) -> List[str]:
        """Parse URLs from configuration"""
        urls = []
        
        # Try to parse lambda_api_urls (JSON array)
        if config.lambda_api_urls:
            try:
                urls = json.loads(config.lambda_api_urls)
                if isinstance(urls, list):
                    # Filter out empty URLs
                    urls = [url.strip() for url in urls if url and url.strip()]
            except (json.JSONDecodeError, TypeError):
                urls = []
        
        # Fallback to single URL for backward compatibility
        if not urls and config.lambda_api_url:
            urls = [config.lambda_api_url.strip()]
        
        return urls
    
    async def get_next_url(self) -> Optional[str]:
        """Get next URL using round-robin polling"""
        config = await self._get_config()
        
        if not config.lambda_enabled:
            return None
        
        urls = self._parse_urls(config)
        if not urls:
            return None
        
        async with self._lock:
            # Round-robin selection
            url = urls[self._current_index % len(urls)]
            self._current_index = (self._current_index + 1) % len(urls)
            return url
    
    async def get_api_key(self) -> Optional[str]:
        """Get Lambda API key"""
        config = await self._get_config()
        return config.lambda_api_key if config.lambda_enabled else None
    
    async def is_enabled(self) -> bool:
        """Check if Lambda is enabled"""
        config = await self._get_config()
        return config.lambda_enabled
    
    async def get_all_urls(self) -> List[str]:
        """Get all configured URLs"""
        config = await self._get_config()
        return self._parse_urls(config) if config.lambda_enabled else []
    
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
        
        urls = await self.get_all_urls()
        api_key = await self.get_api_key()
        
        if not urls:
            raise HTTPException(status_code=400, detail="No Lambda URLs configured")
        
        if not api_key:
            raise HTTPException(status_code=400, detail="Lambda API key not configured")
        
        # Try each URL in round-robin order
        last_error = None
        for attempt in range(len(urls)):
            url = await self.get_next_url()
            if not url:
                continue
            
            try:
                task_id = await self._post_create_task(url, api_key, token, payload)
                print(f"✅ [Lambda] Task created successfully using {url}: {task_id}")
                return task_id
            except Exception as e:
                last_error = e
                print(f"⚠️ [Lambda] Failed to create task using {url}: {str(e)}")
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