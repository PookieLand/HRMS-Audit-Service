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
    JWT_AUDIENCE: str | None = (
        None  # Set to None to skip audience validation, or set to your client_id
    )
    JWT_ISSUER: str | None = (
        None  # Set to None to skip issuer validation, or set to expected issuer
    )

    # Kafka Configuration
    KAFKA_ENABLED: bool = True
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_GROUP_ID: str = "audit-service-group"
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"  # earliest, latest
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 1000
    KAFKA_SESSION_TIMEOUT_MS: int = 30000
    KAFKA_MAX_POLL_INTERVAL_MS: int = 300000

    # Kafka Topics to consume
    KAFKA_TOPICS: str = "user-events,employee-events,attendance-events,leave-events"

    @property
    def kafka_topics_list(self) -> List[str]:
        """Parse KAFKA_TOPICS from comma-separated string."""
        if isinstance(self.KAFKA_TOPICS, str):
            return [topic.strip() for topic in self.KAFKA_TOPICS.split(",")]
        return [self.KAFKA_TOPICS]

    # Audit Service Specific Settings
    RETENTION_DAYS: int = 365  # How many days to retain audit logs
    MAX_BATCH_SIZE: int = 1000  # Maximum batch size for bulk operations
    ENABLE_ENCRYPTION: bool = False  # Whether to encrypt sensitive data in logs
    LOG_SENSITIVE_DATA: bool = False  # Whether to log potentially sensitive data

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


# Create global settings instance
settings = Settings()
