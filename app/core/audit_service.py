"""
Audit Service module.
Handles creation of audit logs from Kafka events with comprehensive event mapping.
Includes deduplication, RBAC considerations, and better event parsing.
"""

import json
from datetime import datetime
from typing import Any, Optional

from app.core.cache import get_cache_service
from app.core.database import get_session
from app.core.logging import get_logger
from app.core.topics import KafkaTopics
from app.models.audit import AuditLog, AuditLogType

logger = get_logger(__name__)


# Comprehensive event type to audit log type mapping
EVENT_TYPE_MAPPING = {
    # User events
    "user.created": AuditLogType.EMPLOYEE_CREATE,
    "user.updated": AuditLogType.EMPLOYEE_UPDATE,
    "user.deleted": AuditLogType.EMPLOYEE_DELETE,
    "user.suspended": AuditLogType.PERMISSION_CHANGE,
    "user.activated": AuditLogType.PERMISSION_CHANGE,
    "user.role.changed": AuditLogType.ROLE_ASSIGNMENT,
    "user.login": AuditLogType.LOGIN,
    "user.logout": AuditLogType.LOGOUT,
    # Onboarding events
    "user.onboarding.initiated": AuditLogType.EMPLOYEE_CREATE,
    "user.onboarding.asgardeo.created": AuditLogType.EMPLOYEE_CREATE,
    "user.onboarding.employee.created": AuditLogType.EMPLOYEE_CREATE,
    "user.onboarding.completed": AuditLogType.EMPLOYEE_CREATE,
    "user.onboarding.failed": AuditLogType.SYSTEM,
    # Employee events
    "employee.created": AuditLogType.EMPLOYEE_CREATE,
    "employee.updated": AuditLogType.EMPLOYEE_UPDATE,
    "employee.deleted": AuditLogType.EMPLOYEE_DELETE,
    "employee.terminated": AuditLogType.EMPLOYEE_DELETE,
    "employee.promoted": AuditLogType.EMPLOYEE_UPDATE,
    "employee.transferred": AuditLogType.EMPLOYEE_UPDATE,
    "employee.probation.started": AuditLogType.EMPLOYEE_UPDATE,
    "employee.probation.completed": AuditLogType.EMPLOYEE_UPDATE,
    "employee.contract.started": AuditLogType.EMPLOYEE_CREATE,
    "employee.contract.renewed": AuditLogType.EMPLOYEE_UPDATE,
    "employee.contract.ended": AuditLogType.EMPLOYEE_UPDATE,
    "employee.salary.updated": AuditLogType.PAYROLL,
    "employee.salary.increment": AuditLogType.PAYROLL,
    "employee.department.changed": AuditLogType.EMPLOYEE_UPDATE,
    "employee.team.changed": AuditLogType.EMPLOYEE_UPDATE,
    "employee.manager.changed": AuditLogType.EMPLOYEE_UPDATE,
    # Attendance events
    "attendance.checkin": AuditLogType.ATTENDANCE,
    "attendance.checkout": AuditLogType.ATTENDANCE,
    "attendance.updated": AuditLogType.ATTENDANCE,
    "attendance.deleted": AuditLogType.ATTENDANCE,
    "attendance.marked": AuditLogType.ATTENDANCE,
    "attendance.late": AuditLogType.ATTENDANCE,
    "attendance.early.departure": AuditLogType.ATTENDANCE,
    "attendance.overtime": AuditLogType.ATTENDANCE,
    "attendance.short.leave": AuditLogType.ATTENDANCE,
    "attendance.absent": AuditLogType.ATTENDANCE,
    # Leave events
    "leave.requested": AuditLogType.LEAVE_REQUEST,
    "leave.approved": AuditLogType.LEAVE_APPROVAL,
    "leave.rejected": AuditLogType.LEAVE_APPROVAL,
    "leave.cancelled": AuditLogType.LEAVE_REQUEST,
    "leave.modified": AuditLogType.LEAVE_REQUEST,
    "leave.revoked": AuditLogType.LEAVE_APPROVAL,
    "leave.started": AuditLogType.LEAVE_REQUEST,
    "leave.ended": AuditLogType.LEAVE_REQUEST,
    "leave.extended": AuditLogType.LEAVE_REQUEST,
    "leave.created": AuditLogType.LEAVE_REQUEST,
    "leave.updated": AuditLogType.LEAVE_REQUEST,
    "leave.balance.updated": AuditLogType.LEAVE_REQUEST,
    "leave.balance.reset": AuditLogType.LEAVE_REQUEST,
    "leave.accrued": AuditLogType.LEAVE_REQUEST,
    # Notification events
    "notification.sent": AuditLogType.SYSTEM,
    "notification.failed": AuditLogType.SYSTEM,
    "notification.delivered": AuditLogType.SYSTEM,
    # Compliance events
    "compliance.check": AuditLogType.SYSTEM,
    "compliance.report": AuditLogType.SYSTEM,
    # Policy events
    "policy.created": AuditLogType.POLICY_CREATE,
    "policy.updated": AuditLogType.POLICY_UPDATE,
    "policy.deleted": AuditLogType.POLICY_DELETE,
    # Document events
    "document.uploaded": AuditLogType.DOCUMENT_UPLOAD,
    "document.deleted": AuditLogType.DOCUMENT_DELETE,
    # Data operations
    "data.exported": AuditLogType.EXPORT,
    "data.imported": AuditLogType.IMPORT,
}

# Topic to event type prefix mapping
TOPIC_PREFIX_MAPPING = {
    "user-": "user",
    "employee-": "employee",
    "attendance-": "attendance",
    "leave-": "leave",
    "notification-": "notification",
    "compliance-": "compliance",
    "audit-": "audit",
}


def map_event_to_log_type(event_type: str) -> AuditLogType:
    """
    Map Kafka event type to audit log type.

    Args:
        event_type: The event type string (e.g., "user.created", "leave.approved")

    Returns:
        Corresponding AuditLogType enum value
    """
    # Try direct mapping first
    log_type = EVENT_TYPE_MAPPING.get(event_type)
    if log_type:
        return log_type

    # Try normalized version (replace - with .)
    normalized = event_type.replace("-", ".").lower()
    log_type = EVENT_TYPE_MAPPING.get(normalized)
    if log_type:
        return log_type

    # Try to infer from event type parts
    parts = event_type.replace("-", ".").split(".")
    if len(parts) >= 2:
        domain = parts[0].lower()
        action = parts[-1].lower()

        # Map domain to log type
        if domain in ["user", "employee"]:
            if action in ["created", "create"]:
                return AuditLogType.EMPLOYEE_CREATE
            elif action in ["updated", "update"]:
                return AuditLogType.EMPLOYEE_UPDATE
            elif action in ["deleted", "delete", "terminated"]:
                return AuditLogType.EMPLOYEE_DELETE
        elif domain == "attendance":
            return AuditLogType.ATTENDANCE
        elif domain == "leave":
            if action in ["approved", "rejected", "revoked"]:
                return AuditLogType.LEAVE_APPROVAL
            else:
                return AuditLogType.LEAVE_REQUEST
        elif domain == "payroll" or "salary" in event_type:
            return AuditLogType.PAYROLL

    return AuditLogType.OTHER


def extract_event_type_from_topic(topic: str, event_data: dict) -> str:
    """
    Extract or construct event type from topic and event data.

    Args:
        topic: Kafka topic name
        event_data: Event payload dictionary

    Returns:
        Event type string
    """
    # First, try to get event_type from the event data
    event_type = event_data.get("event_type")
    if event_type:
        return event_type

    # Try to construct from topic name
    # Convert topic like "user-created" to "user.created"
    return topic.replace("-", ".")


def extract_entity_info(event_data: dict, topic: str) -> tuple[str, str]:
    """
    Extract entity type and ID from event data.

    Args:
        event_data: Event payload dictionary
        topic: Kafka topic name (for fallback)

    Returns:
        Tuple of (entity_type, entity_id)
    """
    data = event_data.get("data", event_data)

    # Determine entity type from topic
    entity_type = "unknown"
    for prefix, etype in TOPIC_PREFIX_MAPPING.items():
        if topic.startswith(prefix):
            entity_type = etype
            break

    # If still unknown, try to extract from event_type
    event_type = event_data.get("event_type", "")
    if entity_type == "unknown" and event_type:
        parts = event_type.split(".")
        if parts:
            entity_type = parts[0]

    # Extract entity ID
    entity_id_keys = [
        f"{entity_type}_id",
        "id",
        "entity_id",
        "record_id",
        "user_id",
        "employee_id",
        "attendance_id",
        "leave_id",
        "onboarding_id",
    ]

    entity_id = ""
    for key in entity_id_keys:
        if key in data:
            entity_id = str(data[key])
            break

    return entity_type, entity_id


def extract_user_info(event_data: dict) -> tuple[str, Optional[str]]:
    """
    Extract user ID and role from event data.

    Args:
        event_data: Event payload dictionary

    Returns:
        Tuple of (user_id, role)
    """
    data = event_data.get("data", event_data)
    metadata = event_data.get("metadata", {})

    # Try multiple sources for user_id
    user_id = (
        metadata.get("actor_user_id")
        or metadata.get("user_id")
        or data.get("performed_by")
        or data.get("user_id")
        or data.get("actor_user_id")
        or "system"
    )

    # Try to get role
    role = (
        metadata.get("actor_role")
        or metadata.get("role")
        or data.get("actor_role")
        or data.get("role")
    )

    return str(user_id), role


def parse_timestamp(event_data: dict) -> datetime:
    """
    Parse timestamp from event data.

    Args:
        event_data: Event payload dictionary

    Returns:
        Parsed datetime or current UTC time
    """
    timestamp_str = event_data.get("timestamp")
    if timestamp_str:
        try:
            # Handle ISO format with Z suffix
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            return datetime.fromisoformat(timestamp_str)
        except (ValueError, AttributeError):
            pass

    return datetime.utcnow()


def check_duplicate(event_id: str) -> bool:
    """
    Check if an event has already been processed using Redis.

    Args:
        event_id: Unique event identifier

    Returns:
        True if duplicate, False if new
    """
    try:
        cache = get_cache_service()
        return cache.is_duplicate_event(event_id)
    except Exception as e:
        logger.warning(f"Deduplication check failed, processing event: {e}")
        return False


def handle_kafka_event(event: dict, topic: str = "unknown") -> None:
    """
    Handle a Kafka event and create an audit log entry.
    This is called synchronously from the consumer thread.

    Args:
        event: The event dictionary from Kafka
        topic: The Kafka topic the event came from
    """
    try:
        # Extract event ID for deduplication
        event_id = event.get("event_id", event.get("id"))

        # Check for duplicates
        if event_id and check_duplicate(event_id):
            logger.debug(f"Duplicate event {event_id}, skipping")
            return

        # Extract event type
        event_type = extract_event_type_from_topic(topic, event)

        # Extract data and metadata
        data = event.get("data", event)
        metadata = event.get("metadata", {})

        # Extract entity information
        entity_type, entity_id = extract_entity_info(event, topic)

        # Extract action from event type
        parts = event_type.split(".")
        action = parts[-1] if len(parts) > 1 else event_type

        # Extract user information
        user_id, actor_role = extract_user_info(event)

        # Parse timestamp
        timestamp = parse_timestamp(event)

        # Map to log type
        log_type = map_event_to_log_type(event_type)

        # Get service name
        service_name = metadata.get(
            "source_service",
            metadata.get("service", KafkaTopics.get_topic_domain(topic) + "-service"),
        )

        # Build description
        description = f"{event_type} event processed from topic {topic}"
        if data.get("description"):
            description = data.get("description")

        # Create audit log entry
        audit_log = AuditLog(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            service_name=service_name,
            log_type=log_type,
            status="success",
            description=description,
            timestamp=timestamp,
            ip_address=metadata.get("ip_address"),
            user_agent=metadata.get("user_agent"),
            request_id=metadata.get("correlation_id", event_id),
            old_values=_safe_json_str(
                data.get("old_values") or data.get("previous_values")
            ),
            new_values=_safe_json_str(data.get("new_values") or data),
        )

        # Get a database session and save
        session = next(get_session())
        try:
            session.add(audit_log)
            session.commit()
            session.refresh(audit_log)

            # Invalidate relevant caches
            try:
                cache = get_cache_service()
                cache.invalidate_audit_cache()
            except Exception:
                pass  # Cache invalidation is not critical

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


def _safe_json_str(value: Any) -> Optional[str]:
    """
    Safely convert a value to JSON string.

    Args:
        value: Value to convert

    Returns:
        JSON string or None
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
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
    description: Optional[str] = None,
    old_values: Optional[str] = None,
    new_values: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    request_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Optional[AuditLog]:
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

            # Invalidate cache
            try:
                cache = get_cache_service()
                cache.invalidate_audit_cache()
            except Exception:
                pass

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


def get_audit_statistics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Get audit log statistics for the given date range.

    Args:
        start_date: Start of date range (optional)
        end_date: End of date range (optional)

    Returns:
        Dictionary with statistics
    """
    from sqlmodel import func, select

    session = next(get_session())
    try:
        # Build base query
        query = select(
            AuditLog.log_type,
            func.count(AuditLog.id).label("count"),
        ).group_by(AuditLog.log_type)

        if start_date:
            query = query.where(AuditLog.timestamp >= start_date)
        if end_date:
            query = query.where(AuditLog.timestamp <= end_date)

        results = session.exec(query).all()

        # Convert to dictionary
        by_type = {str(row[0].value): row[1] for row in results}
        total = sum(by_type.values())

        return {
            "total": total,
            "by_type": by_type,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
        }
    except Exception as e:
        logger.error(f"Error getting audit statistics: {e}")
        return {"total": 0, "by_type": {}, "error": str(e)}
    finally:
        session.close()
