from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.audit import router as audit_logs_router
from app.core.config import settings
from app.core.database import create_db_and_tables
from app.core.kafka import KafkaConsumer
from app.core.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup
    logger.info("Starting Audit Service...")
    logger.info("Creating database and tables...")
    create_db_and_tables()
    logger.info("Database and tables created successfully")

    logger.info("Initializing Kafka consumer...")
    await KafkaConsumer.start()
    logger.info("Kafka consumer initialized successfully")

    logger.info("Audit Service startup complete")

    yield

    # Shutdown
    logger.info("Audit Service shutting down...")
    await KafkaConsumer.stop()
    logger.info("Kafka consumer stopped")


# Initialize FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
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
app.include_router(audit_logs_router, prefix="/api/v1")


@app.get("/health", tags=["health"])
async def detailed_health_check():
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
    }
