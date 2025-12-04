from datetime import datetime

from app.core.database import get_session
from app.core.logging import get_logger
from app.models.audit import AuditLog, AuditLogType

logger = get_logger(__name__)


def map_event_to_log_type(event_type: str) -> AuditLogType:
    """Map Kafka event type to audit log type."""
    mapping = {
        "user.created": AuditLogType.user_management,
        "user.updated": AuditLogType.user_management,
        "user.deleted": AuditLogType.user_management,
        "user.suspended": AuditLogType.user_management,
        "user.activated": AuditLogType.user_management,
        "user.role.changed": AuditLogType.user_management,
        "employee.created": AuditLogType.employee_management,
        "employee.updated": AuditLogType.employee_management,
        "employee.deleted": AuditLogType.employee_management,
        "employee.promoted": AuditLogType.employee_management,
        "attendance.marked": AuditLogType.attendance,
        "attendance.updated": AuditLogType.attendance,
        "attendance.deleted": AuditLogType.attendance,
        "leave.requested": AuditLogType.leave_request,
        "leave.approved": AuditLogType.leave_request,
        "leave.rejected": AuditLogType.leave_request,
        "leave.cancelled": AuditLogType.leave_request,
    }
    return mapping.get(event_type, AuditLogType.system)


async def create_audit_log_from_event(event: dict):
    """Create audit log from Kafka event."""
    try:
        event_type = event.get("event_type")
        data = event.get("data", {})
        metadata = event.get("metadata", {})

        # Map event to audit log
        audit_log = AuditLog(
            user_id=data.get("user_id") or metadata.get("user_id"),
            action=event_type.split(".")[-1],  # e.g., "created" from "user.created"
            entity_type=event_type.split(".")[0],  # e.g., "user" from "user.created"
            entity_id=str(data.get(f"{event_type.split('.')[0]}_id", "")),
            service_name=metadata.get("source_service", "unknown"),
            log_type=map_event_to_log_type(event_type),
            status="success",
            description=f"{event_type} event processed",
            timestamp=datetime.fromisoformat(
                event.get("timestamp", datetime.utcnow().isoformat())
            ),
        )

        # Add session and commit
        session = next(get_session())
        session.add(audit_log)
        session.commit()
        session.refresh(audit_log)

        logger.info(f"Created audit log from event: {event_type} (ID: {audit_log.id})")

    except Exception as e:
        logger.error(f"Failed to create audit log from event: {e}")
        # Don't raise, just log
