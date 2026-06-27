import redis
from src.config import settings

class RateLimiter:
    def __init__(self):
        self.r = redis.from_url(settings.REDIS_URL)

    def is_allowed(self, customer_id: str, tokens: int = 1500) -> bool:
        key = f"rate_limit:{customer_id}"
        current = self.r.get(key)
        
        if current is None:
            self.r.setex(key, 60, settings.LLM_TOKENS_PER_MINUTE)
            current = settings.LLM_TOKENS_PER_MINUTE
            
        if int(current) < tokens:
            return False
            
        self.r.decrby(key, tokens)
        return True