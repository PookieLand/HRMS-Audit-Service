from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.audit import router as audit_logs_router
from app.core.config import settings
from app.core.database import create_db_and_tables
from app.core.kafka import KafkaConsumer, KafkaProducer
from app.core.kafka import health_check as kafka_health_check
from app.core.logging import get_logger

logger = get_logger(__name__)

# Global state for tracking service health
service_state = {
    "kafka_connected": False,
    "redis_connected": False,
    "database_ready": False,
}


def init_redis():
    """Initialize Redis cache."""
    try:
        from app.core.cache import init_cache

        cache = init_cache()
        service_state["redis_connected"] = cache.is_connected()
        if service_state["redis_connected"]:
            logger.info("Redis cache initialized successfully")
        else:
            logger.warning("Redis cache failed to connect")
    except Exception as e:
        logger.warning(f"Failed to initialize Redis: {e}")
        service_state["redis_connected"] = False


def shutdown_redis():
    """Shutdown Redis connection."""
    try:
        from app.core.cache import close_cache

        close_cache()
        service_state["redis_connected"] = False
        logger.info("Redis cache closed")
    except Exception as e:
        logger.error(f"Error closing Redis: {e}")


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    logger.info("Starting Audit Service...")

    # Initialize database
    logger.info("Creating database and tables...")
    create_db_and_tables()
    service_state["database_ready"] = True
    logger.info("Database and tables created successfully")

    # Initialize Redis cache
    logger.info("Initializing Redis cache...")
    init_redis()

    # Initialize Kafka consumer
    if settings.KAFKA_ENABLED:
        logger.info("Initializing Kafka consumer...")
        try:
            await KafkaConsumer.start()
            service_state["kafka_connected"] = True
            logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            service_state["kafka_connected"] = False
    else:
        logger.info("Kafka is disabled, skipping consumer initialization")

    logger.info("Audit Service startup complete")

    yield

    # Shutdown
    logger.info("Audit Service shutting down...")

    # Stop Kafka consumer
    if service_state["kafka_connected"]:
        await KafkaConsumer.stop()
        KafkaProducer.close()
        service_state["kafka_connected"] = False
        logger.info("Kafka consumer and producer stopped")

    # Close Redis
    if service_state["redis_connected"]:
        shutdown_redis()

    logger.info("Audit Service shutdown complete")


# Initialize FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Audit Service for HRMS - Consumes Kafka events and stores audit logs with RBAC-based access control",
    lifespan=lifespan,
)


# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)


# Include routers
app.include_router(audit_logs_router, prefix="/api/v1/audit")


@app.get("/health", tags=["health"])
async def health_check():
    """
    Detailed health check endpoint with Kafka and Redis status.
    """
    kafka_status = kafka_health_check()

    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "connections": {
            "database": service_state["database_ready"],
            "kafka": service_state["kafka_connected"],
            "redis": service_state["redis_connected"],
        },
        "kafka": kafka_status,
    }


@app.get("/health/ready", tags=["health"])
async def readiness_check():
    """
    Readiness probe for Kubernetes.
    Returns 200 if service is ready to accept traffic.
    """
    is_ready = service_state["database_ready"]

    if is_ready:
        return {"status": "ready"}
    else:
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="Service not ready")


@app.get("/health/live", tags=["health"])
async def liveness_check():
    """
    Liveness probe for Kubernetes.
    Returns 200 if service is alive.
    """
    return {"status": "alive"}


@app.get("/health/kafka", tags=["health"])
async def kafka_health():
    """Kafka connection health check."""
    return {
        "enabled": settings.KAFKA_ENABLED,
        "connected": service_state["kafka_connected"],
        "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group_id": settings.KAFKA_GROUP_ID,
        "details": kafka_health_check(),
    }


@app.get("/health/redis", tags=["health"])
async def redis_health():
    """Redis connection health check."""
    connected = False
    try:
        from app.core.cache import get_cache_service

        cache = get_cache_service()
        connected = cache.is_connected()
    except Exception:
        pass

    return {
        "connected": connected,
        "host": settings.REDIS_HOST,
        "port": settings.REDIS_PORT,
    }


@app.get("/metrics/audit", tags=["metrics"])
async def audit_metrics():
    """
    Get audit log metrics for dashboard.
    """
    try:
        from datetime import date, timedelta

        from app.core.audit_service import get_audit_statistics
        from app.core.cache import get_cache_service

        cache = get_cache_service()
        today = date.today().isoformat()

        # Try to get from cache first
        cached = cache.get_audit_metrics("daily", today)
        if cached:
            return cached

        # Calculate metrics
        from datetime import datetime

        today_start = datetime.combine(date.today(), datetime.min.time())
        week_start = today_start - timedelta(days=7)

        today_stats = get_audit_statistics(start_date=today_start)
        week_stats = get_audit_statistics(start_date=week_start)

        metrics = {
            "date": today,
            "today": today_stats,
            "last_7_days": week_stats,
            "counters": {
                "events_today": cache.get_audit_counter(f"events:{today}"),
            },
        }

        # Cache the metrics
        cache.set_audit_metrics("daily", today, metrics, ttl_seconds=300)

        return metrics
    except Exception as e:
        logger.error(f"Error fetching audit metrics: {e}")
        return {
            "error": "Unable to fetch metrics",
            "detail": str(e),
        }


@app.get("/", tags=["info"])
async def root():
    """Root endpoint with service information."""
    from app.core.topics import KafkaTopics

    subscribed_topics = KafkaTopics.all_subscribed_topics()

    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "description": "Audit Service for HRMS - Consumes Kafka events and stores audit logs",
        "endpoints": {
            "health": "/health",
            "health_ready": "/health/ready",
            "health_live": "/health/live",
            "health_kafka": "/health/kafka",
            "health_redis": "/health/redis",
            "metrics": "/metrics/audit",
            "audit_logs": "/api/v1/audit-logs",
            "docs": "/docs",
        },
        "kafka": {
            "enabled": settings.KAFKA_ENABLED,
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group_id": settings.KAFKA_GROUP_ID,
            "topics_count": len(subscribed_topics),
            "topics_sample": subscribed_topics[:10],
        },
        "redis": {
            "configured": settings.redis_configured,
            "host": settings.REDIS_HOST,
        },
        "retention": {
            "hot_storage_days": settings.RETENTION_DAYS,
            "cold_storage_days": settings.RETENTION_DAYS_COLD,
            "s3_enabled": settings.s3_configured,
        },
    }
