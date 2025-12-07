"""
Audit Log database model.
Defines the SQLModel table structure for the AuditLog entity.
"""

from datetime import datetime
from enum import Enum

from sqlmodel import Field, SQLModel


class AuditLogType(str, Enum):
    """Enumeration of audit log types."""

    # Leave management
    LEAVE_REQUEST = "leave_request"
    LEAVE_APPROVAL = "leave_approval"

    # Attendance
    ATTENDANCE = "attendance"

    # Payroll
    PAYROLL = "payroll"

    # Employee management
    EMPLOYEE_UPDATE = "employee_update"
    EMPLOYEE_CREATE = "employee_create"
    EMPLOYEE_DELETE = "employee_delete"

    # Policy management
    POLICY_UPDATE = "policy_update"
    POLICY_CREATE = "policy_create"
    POLICY_DELETE = "policy_delete"

    # Document management
    DOCUMENT_UPLOAD = "document_upload"
    DOCUMENT_DELETE = "document_delete"

    # Access control
    ROLE_ASSIGNMENT = "role_assignment"
    PERMISSION_CHANGE = "permission_change"

    # Authentication
    LOGIN = "login"
    LOGOUT = "logout"

    # Data operations
    EXPORT = "export"
    IMPORT = "import"

    # User management (alias for compatibility)
    USER_MANAGEMENT = "user_management"
    EMPLOYEE_MANAGEMENT = "employee_management"

    # System/Other
    SYSTEM = "system"
    OTHER = "other"


class AuditLog(SQLModel, table=True):
    """
    AuditLog database table model.
    Represents the audit_logs table in the database with comprehensive audit trail information.
    """

    id: int | None = Field(default=None, primary_key=True)
    user_id: str = Field(index=True, max_length=255, nullable=False)
    action: str = Field(index=True, max_length=255, nullable=False)
    log_type: AuditLogType = Field(index=True, nullable=False)
    entity_type: str = Field(index=True, max_length=255, nullable=False)
    entity_id: str = Field(index=True, max_length=255, nullable=False)
    old_values: str | None = Field(default=None, nullable=True)  # JSON string
    new_values: str | None = Field(default=None, nullable=True)  # JSON string
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, index=True, nullable=False
    )
    ip_address: str | None = Field(default=None, max_length=50, nullable=True)
    user_agent: str | None = Field(default=None, nullable=True)
    service_name: str = Field(index=True, max_length=255, nullable=False)
    request_id: str | None = Field(default=None, max_length=255, nullable=True)
    status: str = Field(default="success", max_length=50, nullable=False)
    error_message: str | None = Field(default=None, nullable=True)
    description: str | None = Field(default=None, nullable=True)

    class Config:
        # Enables indexing for better query performance
        indexes = [
            ("user_id", "log_type", "timestamp"),
            ("entity_type", "entity_id", "timestamp"),
            ("log_type", "timestamp"),
            ("service_name", "timestamp"),
        ]
