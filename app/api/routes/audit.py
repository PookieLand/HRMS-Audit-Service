from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from sqlmodel import and_, select  # type: ignore

from app.api.dependencies import CurrentUserDep, SessionDep
from app.core.logging import get_logger
from app.models.audit import AuditLog, AuditLogType
from app.schemas.employee import (
    AuditLogCreate,
    AuditLogListPublic,
    AuditLogPublic,
    AuditLogUpdate,
)

logger = get_logger(__name__)

# Create router with prefix and tags for better organization
router = APIRouter(
    prefix="/audit-logs",
    tags=["audit-logs"],
    responses={404: {"description": "Audit log not found"}},
)


@router.post("/", response_model=AuditLogPublic, status_code=201)
def create_audit_log(audit_log: AuditLogCreate, session: SessionDep) -> AuditLog:
    """
    Create a new audit log entry.

    This endpoint is called by other microservices to log audit events.

    Args:
        audit_log: Audit log data from request body
        session: Database session (injected)

    Returns:
        Created audit log with generated ID and timestamp

    Example:
        ```
        POST /api/v1/audit-logs
        {
            "user_id": "user123",
            "action": "created",
            "log_type": "leave_request",
            "entity_type": "LeaveRequest",
            "entity_id": "leave456",
            "service_name": "leave-management-service",
            "status": "success",
            "description": "Leave request created successfully",
            "new_values": "{\"status\": \"pending\", \"days\": 5}"
        }
        ```
    """
    logger.info(
        f"Creating new audit log: {audit_log.action} for {audit_log.entity_type}"
    )
    db_audit_log = AuditLog.model_validate(audit_log)
    session.add(db_audit_log)
    session.commit()
    session.refresh(db_audit_log)
    logger.info(f"Audit log created successfully with ID: {db_audit_log.id}")
    return db_audit_log


@router.get("/", response_model=AuditLogListPublic)
def list_audit_logs(
    session: SessionDep,
    offset: int = Query(0, ge=0),
    limit: Annotated[int, Query(le=1000)] = 100,
    user_id: str | None = Query(None),
    log_type: AuditLogType | None = Query(None),
    service_name: str | None = Query(None),
    entity_type: str | None = Query(None),
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
) -> AuditLogListPublic:
    """
    List all audit logs with optional filtering and pagination.

    Args:
        session: Database session (injected)
        offset: Number of records to skip (default: 0)
        limit: Maximum number of records to return (default: 100, max: 1000)
        user_id: Filter by user ID (optional)
        log_type: Filter by log type (optional)
        service_name: Filter by service name (optional)
        entity_type: Filter by entity type (optional)
        start_date: Filter logs from this date onwards (optional)
        end_date: Filter logs up to this date (optional)

    Returns:
        Paginated list of audit logs matching the filters

    Example:
        ```
        GET /api/v1/audit-logs?limit=50&offset=0&log_type=leave_request&service_name=leave-management-service
        ```
    """
    logger.info(
        f"Fetching audit logs with offset={offset}, limit={limit}, "
        f"filters: user_id={user_id}, log_type={log_type}, service_name={service_name}"
    )

    # Build query with filters
    query = select(AuditLog)
    filters = []

    if user_id:
        filters.append(AuditLog.user_id == user_id)
    if log_type:
        filters.append(AuditLog.log_type == log_type)
    if service_name:
        filters.append(AuditLog.service_name == service_name)
    if entity_type:
        filters.append(AuditLog.entity_type == entity_type)
    if start_date:
        filters.append(AuditLog.timestamp >= start_date)
    if end_date:
        filters.append(AuditLog.timestamp <= end_date)

    if filters:
        query = query.where(and_(*filters))

    # Get total count
    count_query = select(AuditLog)
    if filters:
        count_query = count_query.where(and_(*filters))
    total = len(session.exec(count_query).all())

    # Get paginated results ordered by timestamp descending
    audit_logs = session.exec(
        query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(limit)
    ).all()

    logger.info(f"Retrieved {len(audit_logs)} audit log(s) out of {total} total")

    return AuditLogListPublic(
        total=total,
        items=list(audit_logs),
        limit=limit,
        offset=offset,
    )


@router.get("/{audit_id}", response_model=AuditLogPublic)
def get_audit_log(audit_id: int, session: SessionDep) -> AuditLog:
    """
    Retrieve a single audit log by ID.

    Args:
        audit_id: The ID of the audit log to retrieve
        session: Database session (injected)

    Returns:
        Audit log data

    Raises:
        HTTPException: 404 if audit log not found

    Example:
        ```
        GET /api/v1/audit-logs/123
        ```
    """
    logger.info(f"Fetching audit log with ID: {audit_id}")
    audit_log = session.get(AuditLog, audit_id)
    if not audit_log:
        logger.warning(f"Audit log with ID {audit_id} not found")
        raise HTTPException(status_code=404, detail="Audit log not found")
    logger.info(f"Audit log found: {audit_log.action}")
    return audit_log


@router.get("/user/{user_id}", response_model=AuditLogListPublic)
def get_user_audit_logs(
    user_id: str,
    session: SessionDep,
    offset: int = Query(0, ge=0),
    limit: Annotated[int, Query(le=1000)] = 100,
    log_type: AuditLogType | None = Query(None),
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
) -> AuditLogListPublic:
    """
    Retrieve all audit logs for a specific user.

    Args:
        user_id: The ID of the user
        session: Database session (injected)
        offset: Number of records to skip (default: 0)
        limit: Maximum number of records to return (default: 100, max: 1000)
        log_type: Filter by log type (optional)
        start_date: Filter logs from this date onwards (optional)
        end_date: Filter logs up to this date (optional)

    Returns:
        Paginated list of audit logs for the user

    Example:
        ```
        GET /api/v1/audit-logs/user/user123?limit=50
        ```
    """
    logger.info(
        f"Fetching audit logs for user: {user_id}, offset={offset}, limit={limit}"
    )

    query = select(AuditLog).where(AuditLog.user_id == user_id)
    filters = [AuditLog.user_id == user_id]

    if log_type:
        filters.append(AuditLog.log_type == log_type)
    if start_date:
        filters.append(AuditLog.timestamp >= start_date)
    if end_date:
        filters.append(AuditLog.timestamp <= end_date)

    query = select(AuditLog).where(and_(*filters))

    # Get total count
    count_query = select(AuditLog).where(and_(*filters))
    total = len(session.exec(count_query).all())

    # Get paginated results
    audit_logs = session.exec(
        query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(limit)
    ).all()

    logger.info(
        f"Retrieved {len(audit_logs)} audit log(s) for user {user_id} out of {total} total"
    )

    return AuditLogListPublic(
        total=total,
        items=list(audit_logs),
        limit=limit,
        offset=offset,
    )


@router.get("/type/{log_type}", response_model=AuditLogListPublic)
def get_audit_logs_by_type(
    log_type: AuditLogType,
    session: SessionDep,
    offset: int = Query(0, ge=0),
    limit: Annotated[int, Query(le=1000)] = 100,
    service_name: str | None = Query(None),
    user_id: str | None = Query(None),
    start_date: datetime | None = Query(None),
    end_date: datetime | None = Query(None),
) -> AuditLogListPublic:
    """
    Retrieve audit logs filtered by log type (e.g., leave_request, attendance, etc.).

    Args:
        log_type: The type of audit logs to retrieve
        session: Database session (injected)
        offset: Number of records to skip (default: 0)
        limit: Maximum number of records to return (default: 100, max: 1000)
        service_name: Filter by service name (optional)
        user_id: Filter by user ID (optional)
        start_date: Filter logs from this date onwards (optional)
        end_date: Filter logs up to this date (optional)

    Returns:
        Paginated list of audit logs of the specified type

    Example:
        ```
        GET /api/v1/audit-logs/type/leave_request?limit=50&service_name=leave-management-service
        ```
    """
    logger.info(
        f"Fetching audit logs of type: {log_type}, offset={offset}, limit={limit}"
    )

    filters = [AuditLog.log_type == log_type]

    if service_name:
        filters.append(AuditLog.service_name == service_name)
    if user_id:
        filters.append(AuditLog.user_id == user_id)
    if start_date:
        filters.append(AuditLog.timestamp >= start_date)
    if end_date:
        filters.append(AuditLog.timestamp <= end_date)

    query = select(AuditLog).where(and_(*filters))

    # Get total count
    count_query = select(AuditLog).where(and_(*filters))
    total = len(session.exec(count_query).all())

    # Get paginated results
    audit_logs = session.exec(
        query.order_by(AuditLog.timestamp.desc()).offset(offset).limit(limit)
    ).all()

    logger.info(
        f"Retrieved {len(audit_logs)} audit log(s) of type {log_type} out of {total} total"
    )

    return AuditLogListPublic(
        total=total,
        items=list(audit_logs),
        limit=limit,
        offset=offset,
    )


@router.patch("/{audit_id}", response_model=AuditLogPublic)
def update_audit_log(
    audit_id: int, audit_log: AuditLogUpdate, session: SessionDep
) -> AuditLog:
    """
    Update an existing audit log (partial update).

    Typically used to update status or error information after creation.

    Args:
        audit_id: The ID of the audit log to update
        audit_log: Fields to update (only provided fields will be updated)
        session: Database session (injected)

    Returns:
        Updated audit log data

    Raises:
        HTTPException: 404 if audit log not found

    Example:
        ```
        PATCH /api/v1/audit-logs/123
        {
            "status": "failure",
            "error_message": "Database connection failed"
        }
        ```
    """
    logger.info(f"Attempting to update audit log with ID: {audit_id}")
    audit_log_db = session.get(AuditLog, audit_id)
    if not audit_log_db:
        logger.warning(f"Audit log with ID {audit_id} not found for update")
        raise HTTPException(status_code=404, detail="Audit log not found")

    audit_log_data = audit_log.model_dump(exclude_unset=True)
    logger.info(f"Updating audit log {audit_id} with data: {audit_log_data}")
    audit_log_db = audit_log_db.sqlmodel_update(audit_log_data)
    session.add(audit_log_db)
    session.commit()
    session.refresh(audit_log_db)
    logger.info(f"Audit log with ID {audit_id} updated successfully")
    return audit_log_db


@router.delete("/{audit_id}")
def delete_audit_log(audit_id: int, session: SessionDep) -> dict:
    """
    Delete an audit log.

    Warning: Deleting audit logs may violate compliance requirements.
    Use with caution and only with proper authorization.

    Args:
        audit_id: The ID of the audit log to delete
        session: Database session (injected)

    Returns:
        Success confirmation

    Raises:
        HTTPException: 404 if audit log not found

    Example:
        ```
        DELETE /api/v1/audit-logs/123
        ```
    """
    logger.info(f"Attempting to delete audit log with ID: {audit_id}")
    audit_log = session.get(AuditLog, audit_id)
    if not audit_log:
        logger.warning(f"Audit log with ID {audit_id} not found for deletion")
        raise HTTPException(status_code=404, detail="Audit log not found")
    session.delete(audit_log)
    session.commit()
    logger.warning(f"Audit log with ID {audit_id} deleted successfully")
    return {"ok": True}


@router.get("/auth/check")
def check_auth(current_user: CurrentUserDep):
    return {"status": "ok", "username": current_user.username}
