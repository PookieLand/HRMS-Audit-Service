"""
Audit Service module.
Handles creation of audit logs from Kafka events.
"""

from datetime import datetime

from app.core.database import get_session
from app.core.logging import get_logger
from app.models.audit import AuditLog, AuditLogType

logger = get_logger(__name__)


def map_event_to_log_type(event_type: str) -> AuditLogType:
    """Map Kafka event type to audit log type."""
    mapping = {
        # User events
        "user.created": AuditLogType.EMPLOYEE_CREATE,
        "user.updated": AuditLogType.EMPLOYEE_UPDATE,
        "user.deleted": AuditLogType.EMPLOYEE_DELETE,
        "user.suspended": AuditLogType.PERMISSION_CHANGE,
        "user.activated": AuditLogType.PERMISSION_CHANGE,
        "user.role.changed": AuditLogType.ROLE_ASSIGNMENT,
        "user.login": AuditLogType.LOGIN,
        "user.logout": AuditLogType.LOGOUT,
        # Employee events
        "employee.created": AuditLogType.EMPLOYEE_CREATE,
        "employee.updated": AuditLogType.EMPLOYEE_UPDATE,
        "employee.deleted": AuditLogType.EMPLOYEE_DELETE,
        "employee.promoted": AuditLogType.EMPLOYEE_UPDATE,
        # Attendance events
        "attendance.marked": AuditLogType.ATTENDANCE,
        "attendance.updated": AuditLogType.ATTENDANCE,
        "attendance.deleted": AuditLogType.ATTENDANCE,
        "attendance.checkin": AuditLogType.ATTENDANCE,
        "attendance.checkout": AuditLogType.ATTENDANCE,
        # Leave events
        "leave.requested": AuditLogType.LEAVE_REQUEST,
        "leave.approved": AuditLogType.LEAVE_APPROVAL,
        "leave.rejected": AuditLogType.LEAVE_APPROVAL,
        "leave.cancelled": AuditLogType.LEAVE_REQUEST,
        "leave.created": AuditLogType.LEAVE_REQUEST,
        "leave.updated": AuditLogType.LEAVE_REQUEST,
    }
    return mapping.get(event_type, AuditLogType.OTHER)


def handle_kafka_event(event: dict) -> None:
    """
    Handle a Kafka event and create an audit log entry.
    This is called synchronously from the consumer thread.

    Args:
        event: The event dictionary from Kafka
    """
    try:
        event_type = event.get("event_type", "unknown")
        data = event.get("data", {})
        metadata = event.get("metadata", {})

        # Extract entity type and action from event_type
        # Format: "entity.action" (e.g., "user.created", "leave.approved")
        parts = event_type.split(".")
        entity_type = parts[0] if parts else "unknown"
        action = parts[-1] if len(parts) > 1 else event_type

        # Extract entity ID from data
        entity_id_keys = [
            f"{entity_type}_id",
            "id",
            "entity_id",
            "record_id",
        ]
        entity_id = ""
        for key in entity_id_keys:
            if key in data:
                entity_id = str(data[key])
                break

        # Extract user_id - try multiple sources
        user_id = (
            data.get("user_id")
            or data.get("performed_by")
            or metadata.get("user_id")
            or metadata.get("performed_by")
            or "system"
        )

        # Parse timestamp
        timestamp_str = event.get("timestamp")
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                timestamp = datetime.utcnow()
        else:
            timestamp = datetime.utcnow()

        # Create audit log entry
        audit_log = AuditLog(
            user_id=str(user_id),
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            service_name=metadata.get(
                "source_service", metadata.get("service", "unknown")
            ),
            log_type=map_event_to_log_type(event_type),
            status="success",
            description=f"{event_type} event processed",
            timestamp=timestamp,
            ip_address=metadata.get("ip_address"),
            user_agent=metadata.get("user_agent"),
            request_id=metadata.get("request_id", event.get("event_id")),
            old_values=_safe_json_str(data.get("old_values")),
            new_values=_safe_json_str(data.get("new_values")),
        )

        # Get a database session and save
        session = next(get_session())
        try:
            session.add(audit_log)
            session.commit()
            session.refresh(audit_log)
            logger.info(
                f"Created audit log from event: {event_type} "
                f"(ID: {audit_log.id}, entity: {entity_type}/{entity_id})"
            )
        except Exception as db_error:
            session.rollback()
            logger.error(f"Database error creating audit log: {db_error}")
            raise
        finally:
            session.close()

    except Exception as e:
        logger.error(f"Failed to create audit log from event: {e}")
        # Don't re-raise - we don't want to crash the consumer


def _safe_json_str(value) -> str | None:
    """Safely convert a value to JSON string."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        import json

        return json.dumps(value)
    except (TypeError, ValueError):
        return str(value)


def create_audit_log(
    user_id: str,
    action: str,
    entity_type: str,
    entity_id: str,
    service_name: str,
    log_type: AuditLogType,
    status: str = "success",
    description: str | None = None,
    old_values: str | None = None,
    new_values: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    request_id: str | None = None,
    error_message: str | None = None,
) -> AuditLog | None:
    """
    Create an audit log entry directly (not from Kafka event).

    Args:
        user_id: ID of the user who performed the action
        action: The action performed (e.g., "created", "updated")
        entity_type: Type of entity affected (e.g., "user", "employee")
        entity_id: ID of the affected entity
        service_name: Name of the service that performed the action
        log_type: Type of audit log
        status: Status of the action ("success" or "failure")
        description: Optional description
        old_values: JSON string of previous values
        new_values: JSON string of new values
        ip_address: IP address of the request
        user_agent: User agent string
        request_id: Request ID for correlation
        error_message: Error message if status is failure

    Returns:
        Created AuditLog or None if creation failed
    """
    try:
        audit_log = AuditLog(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            service_name=service_name,
            log_type=log_type,
            status=status,
            description=description,
            old_values=old_values,
            new_values=new_values,
            ip_address=ip_address,
            user_agent=user_agent,
            request_id=request_id,
            error_message=error_message,
            timestamp=datetime.utcnow(),
        )

        session = next(get_session())
        try:
            session.add(audit_log)
            session.commit()
            session.refresh(audit_log)
            logger.info(f"Created audit log: {action} on {entity_type}/{entity_id}")
            return audit_log
        except Exception as db_error:
            session.rollback()
            logger.error(f"Database error creating audit log: {db_error}")
            return None
        finally:
            session.close()

    except Exception as e:
        logger.error(f"Failed to create audit log: {e}")
        return None
