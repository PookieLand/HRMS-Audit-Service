from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application Settings
    APP_NAME: str = "Audit Service"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Database Settings
    DB_NAME: str = "audit_service_db"
    DB_USER: str = "root"
    DB_PASSWORD: str = "root"
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_CHARSET: str = "utf8"

    # CORS Settings
    CORS_ORIGINS: str = "https://localhost,http://localhost:3000"
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: List[str] = ["*"]
    CORS_ALLOW_HEADERS: List[str] = ["*"]

    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS_ORIGINS from comma-separated string."""
        if isinstance(self.CORS_ORIGINS, str):
            return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
        return [self.CORS_ORIGINS]

    # JWT/JWKS Settings
    JWKS_URL: str = "https://api.asgardeo.io/t/pookieland/oauth2/jwks"
    JWT_AUDIENCE: str | None = None
    JWT_ISSUER: str | None = None

    # ==========================================
    # Kafka Configuration
    # ==========================================
    KAFKA_ENABLED: bool = True
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_GROUP_ID: str = "audit-service-group"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 1000
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_MAX_POLL_INTERVAL_MS: int = 300000

    # Kafka Topics to consume (comma-separated)
    # These are the audit event topics from all services
    KAFKA_TOPICS: str = (
        "audit-user-action,"
        "audit-employee-action,"
        "audit-attendance-action,"
        "audit-leave-action,"
        "audit-notification-action,"
        "audit-compliance-action,"
        "user-created,"
        "user-updated,"
        "user-deleted,"
        "user-suspended,"
        "user-activated,"
        "user-role-changed,"
        "user-onboarding-initiated,"
        "user-onboarding-completed,"
        "user-onboarding-failed,"
        "employee-created,"
        "employee-updated,"
        "employee-deleted,"
        "employee-terminated,"
        "employee-promoted,"
        "employee-transferred,"
        "employee-salary-updated,"
        "employee-salary-increment,"
        "attendance-checkin,"
        "attendance-checkout,"
        "attendance-updated,"
        "attendance-late,"
        "attendance-absent,"
        "leave-requested,"
        "leave-approved,"
        "leave-rejected,"
        "leave-cancelled,"
        "notification-sent,"
        "notification-failed"
    )

    @property
    def kafka_topics_list(self) -> List[str]:
        """Parse KAFKA_TOPICS from comma-separated string."""
        if isinstance(self.KAFKA_TOPICS, str):
            return [
                topic.strip() for topic in self.KAFKA_TOPICS.split(",") if topic.strip()
            ]
        return [self.KAFKA_TOPICS]

    # ==========================================
    # Redis Configuration
    # ==========================================
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    # Cache TTL Settings (in seconds)
    CACHE_TTL_AUDIT_QUERY: int = 60  # 1 minute for query results
    CACHE_TTL_AUDIT_LOG: int = 300  # 5 minutes for single log
    CACHE_TTL_METRICS: int = 300  # 5 minutes for metrics
    CACHE_TTL_RECENT_EVENTS: int = 30  # 30 seconds for dashboard
    CACHE_TTL_DEDUP: int = 86400  # 24 hours for event deduplication

    @property
    def redis_configured(self) -> bool:
        """Check if Redis is properly configured."""
        return bool(self.REDIS_HOST and self.REDIS_PORT)

    # ==========================================
    # Audit Service Specific Settings
    # ==========================================
    RETENTION_DAYS: int = 30  # Days to keep in hot storage (per CLAUDE.md spec)
    RETENTION_DAYS_COLD: int = 365  # Days to keep in cold storage (S3)
    MAX_BATCH_SIZE: int = 1000  # Maximum batch size for bulk operations
    ENABLE_ENCRYPTION: bool = False  # Whether to encrypt sensitive data in logs
    LOG_SENSITIVE_DATA: bool = False  # Whether to log potentially sensitive data

    # S3 Configuration for long-term storage
    S3_ENABLED: bool = False
    S3_BUCKET: str = ""
    S3_REGION: str = "us-east-1"
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_PREFIX: str = "audit-logs/"

    @property
    def s3_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return bool(
            self.S3_ENABLED
            and self.S3_BUCKET
            and self.S3_ACCESS_KEY
            and self.S3_SECRET_KEY
        )

    @property
    def database_url(self) -> str:
        """Generate MySQL database URL."""
        return f"mysql+mysqldb://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?charset={self.DB_CHARSET}"

    @property
    def database_url_without_db(self) -> str:
        """Generate MySQL URL without database name (for initial connection)."""
        return f"mysql+mysqldb://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}?charset={self.DB_CHARSET}"

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Allow extra env vars without validation errors


# Create global settings instance
settings = Settings()
