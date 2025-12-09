"""
Redis Cache Module for Audit Service.

Provides caching utilities for frequently accessed audit logs and metrics.
Uses Redis for high-performance caching with TTL support.
"""

import json
from datetime import date, datetime
from typing import Any, Optional

import redis

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def json_serializer(obj: Any) -> str:
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class CacheService:
    """
    Redis cache service for the Audit Service.
    Handles caching of audit log queries and metrics.
    """

    def __init__(self):
        self._client: Optional[redis.Redis] = None
        self._connected = False

    def connect(self) -> None:
        """Initialize Redis connection."""
        if self._client is not None and self._connected:
            return

        try:
            self._client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD or None,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            # Test connection
            self._client.ping()
            self._connected = True
            logger.info(
                f"Redis connected to {settings.REDIS_HOST}:{settings.REDIS_PORT}"
            )
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._connected = False
        except Exception as e:
            logger.error(f"Unexpected error connecting to Redis: {e}")
            self._connected = False

    def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client is not None:
            try:
                self._client.close()
                self._connected = False
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        if self._client is None:
            return False
        try:
            self._client.ping()
            return True
        except Exception:
            return False

    def _ensure_connected(self) -> bool:
        """Ensure Redis is connected before operations."""
        if not self._connected or self._client is None:
            self.connect()
        return self._connected

    # ==========================================
    # Generic Cache Operations
    # ==========================================

    def get(self, key: str) -> Optional[str]:
        """Get a value from cache."""
        if not self._ensure_connected():
            return None
        try:
            return self._client.get(key)
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None

    def set(
        self,
        key: str,
        value: str,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """Set a value in cache with optional TTL."""
        if not self._ensure_connected():
            return False
        try:
            if ttl_seconds:
                self._client.setex(key, ttl_seconds, value)
            else:
                self._client.set(key, value)
            return True
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if not self._ensure_connected():
            return False
        try:
            self._client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists in cache."""
        if not self._ensure_connected():
            return False
        try:
            return bool(self._client.exists(key))
        except Exception as e:
            logger.error(f"Cache exists error for key {key}: {e}")
            return False

    def get_json(self, key: str) -> Optional[dict[str, Any]]:
        """Get a JSON value from cache."""
        value = self.get(key)
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for key {key}: {e}")
            return None

    def set_json(
        self,
        key: str,
        value: dict[str, Any],
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """Set a JSON value in cache."""
        try:
            json_str = json.dumps(value, default=json_serializer)
            return self.set(key, json_str, ttl_seconds)
        except (TypeError, json.JSONEncoder) as e:
            logger.error(f"JSON encode error for key {key}: {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching a pattern."""
        if not self._ensure_connected():
            return 0
        try:
            keys = self._client.keys(pattern)
            if keys:
                return self._client.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Cache delete pattern error for {pattern}: {e}")
            return 0

    # ==========================================
    # Audit Log Query Caching
    # ==========================================

    def _audit_query_key(
        self,
        user_id: Optional[str] = None,
        log_type: Optional[str] = None,
        service_name: Optional[str] = None,
        entity_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> str:
        """Generate cache key for audit log queries."""
        parts = [
            "audit:query",
            f"u:{user_id or 'all'}",
            f"t:{log_type or 'all'}",
            f"s:{service_name or 'all'}",
            f"e:{entity_type or 'all'}",
            f"o:{offset}",
            f"l:{limit}",
        ]
        return ":".join(parts)

    def get_cached_audit_logs(
        self,
        user_id: Optional[str] = None,
        log_type: Optional[str] = None,
        service_name: Optional[str] = None,
        entity_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Optional[dict[str, Any]]:
        """Get cached audit log query results."""
        key = self._audit_query_key(
            user_id, log_type, service_name, entity_type, offset, limit
        )
        return self.get_json(key)

    def cache_audit_logs(
        self,
        results: dict[str, Any],
        user_id: Optional[str] = None,
        log_type: Optional[str] = None,
        service_name: Optional[str] = None,
        entity_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        ttl_seconds: int = 60,
    ) -> bool:
        """Cache audit log query results."""
        key = self._audit_query_key(
            user_id, log_type, service_name, entity_type, offset, limit
        )
        return self.set_json(key, results, ttl_seconds)

    def invalidate_audit_cache(self) -> int:
        """Invalidate all cached audit log queries."""
        return self.delete_pattern("audit:query:*")

    # ==========================================
    # Single Audit Log Caching
    # ==========================================

    def _audit_log_key(self, audit_id: int) -> str:
        """Generate cache key for a single audit log."""
        return f"audit:log:{audit_id}"

    def get_cached_audit_log(self, audit_id: int) -> Optional[dict[str, Any]]:
        """Get a cached single audit log."""
        key = self._audit_log_key(audit_id)
        return self.get_json(key)

    def cache_audit_log(
        self,
        audit_id: int,
        audit_log: dict[str, Any],
        ttl_seconds: int = 300,
    ) -> bool:
        """Cache a single audit log."""
        key = self._audit_log_key(audit_id)
        return self.set_json(key, audit_log, ttl_seconds)

    def invalidate_audit_log(self, audit_id: int) -> bool:
        """Invalidate a cached single audit log."""
        key = self._audit_log_key(audit_id)
        return self.delete(key)

    # ==========================================
    # Audit Metrics Caching
    # ==========================================

    def _metrics_key(self, metric_type: str, date_str: str) -> str:
        """Generate cache key for audit metrics."""
        return f"audit:metrics:{metric_type}:{date_str}"

    def get_audit_metrics(
        self,
        metric_type: str,
        date_str: str,
    ) -> Optional[dict[str, Any]]:
        """Get cached audit metrics."""
        key = self._metrics_key(metric_type, date_str)
        return self.get_json(key)

    def set_audit_metrics(
        self,
        metric_type: str,
        date_str: str,
        metrics: dict[str, Any],
        ttl_seconds: int = 300,
    ) -> bool:
        """Cache audit metrics."""
        key = self._metrics_key(metric_type, date_str)
        return self.set_json(key, metrics, ttl_seconds)

    def increment_audit_counter(
        self,
        counter_name: str,
        amount: int = 1,
    ) -> Optional[int]:
        """Increment an audit counter."""
        if not self._ensure_connected():
            return None
        try:
            key = f"audit:counter:{counter_name}"
            return self._client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Error incrementing counter {counter_name}: {e}")
            return None

    def get_audit_counter(self, counter_name: str) -> int:
        """Get an audit counter value."""
        if not self._ensure_connected():
            return 0
        try:
            key = f"audit:counter:{counter_name}"
            value = self._client.get(key)
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"Error getting counter {counter_name}: {e}")
            return 0

    # ==========================================
    # User Access Cache (for RBAC)
    # ==========================================

    def _user_access_key(self, user_id: str) -> str:
        """Generate cache key for user access info."""
        return f"audit:access:{user_id}"

    def get_user_access(self, user_id: str) -> Optional[dict[str, Any]]:
        """Get cached user access information."""
        key = self._user_access_key(user_id)
        return self.get_json(key)

    def cache_user_access(
        self,
        user_id: str,
        access_info: dict[str, Any],
        ttl_seconds: int = 600,
    ) -> bool:
        """Cache user access information."""
        key = self._user_access_key(user_id)
        return self.set_json(key, access_info, ttl_seconds)

    def invalidate_user_access(self, user_id: str) -> bool:
        """Invalidate cached user access information."""
        key = self._user_access_key(user_id)
        return self.delete(key)

    # ==========================================
    # Recent Audit Events Cache (for dashboard)
    # ==========================================

    def _recent_events_key(self, scope: str = "global") -> str:
        """Generate cache key for recent audit events."""
        return f"audit:recent:{scope}"

    def get_recent_events(
        self,
        scope: str = "global",
    ) -> Optional[list[dict[str, Any]]]:
        """Get cached recent audit events."""
        key = self._recent_events_key(scope)
        data = self.get_json(key)
        if data and "events" in data:
            return data["events"]
        return None

    def cache_recent_events(
        self,
        events: list[dict[str, Any]],
        scope: str = "global",
        ttl_seconds: int = 30,
    ) -> bool:
        """Cache recent audit events."""
        key = self._recent_events_key(scope)
        return self.set_json(key, {"events": events}, ttl_seconds)

    # ==========================================
    # Event Deduplication
    # ==========================================

    def _dedup_key(self, event_id: str) -> str:
        """Generate cache key for event deduplication."""
        return f"audit:dedup:{event_id}"

    def is_duplicate_event(
        self,
        event_id: str,
        ttl_seconds: int = 86400,
    ) -> bool:
        """
        Check if an event has already been processed.

        Args:
            event_id: Unique event identifier
            ttl_seconds: How long to remember the event (default 24h)

        Returns:
            True if duplicate, False if new
        """
        if not self._ensure_connected():
            return False  # Fail open - process the event

        try:
            key = self._dedup_key(event_id)
            # SET NX returns True if key was set (new), False if exists (duplicate)
            was_set = self._client.set(key, "1", nx=True, ex=ttl_seconds)
            return not was_set
        except Exception as e:
            logger.error(f"Deduplication check error for {event_id}: {e}")
            return False  # Fail open - process the event


# Global cache service instance
_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    """Get or create the global cache service instance."""
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service


def init_cache() -> CacheService:
    """Initialize and connect the cache service."""
    cache = get_cache_service()
    cache.connect()
    return cache


def close_cache() -> None:
    """Close the cache service connection."""
    global _cache_service
    if _cache_service is not None:
        _cache_service.disconnect()
        _cache_service = None
