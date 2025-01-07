from abc import ABC, abstractmethod
from typing import Any, Optional
import hashlib
import pickle
from datetime import datetime, timedelta

class CacheManager(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        pass

class InMemoryCacheManager(CacheManager):
    def __init__(self):
        self._cache = {}
        self._expiry = {}

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None
        
        if key in self._expiry and datetime.now() > self._expiry[key]:
            del self._cache[key]
            del self._expiry[key]
            return None
            
        return self._cache[key]

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self._cache[key] = value
        if ttl:
            self._expiry[key] = datetime.now() + timedelta(seconds=ttl)
