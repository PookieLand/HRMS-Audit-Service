"""
Role-Based Access Control (RBAC) module for Audit Service.

Implements access control rules for viewing audit logs based on user roles.
Rules follow the CLAUDE.md specification:
- HR_Admin: Can see everything
- HR_Manager: Can see audit events of ones below (manager, employee) but not other HR_Managers
- Manager: Can see audit events of employees only
- Employee: Only generates events, cannot view audit logs
"""

from enum import Enum
from typing import Optional

from app.core.logging import get_logger

logger = get_logger(__name__)


class Role(str, Enum):
    """User roles in the HRMS system."""

    HR_ADMIN = "HR_Admin"
    HR_MANAGER = "HR_Manager"
    MANAGER = "manager"
    EMPLOYEE = "employee"


class AuditDomain(str, Enum):
    """Audit event domains."""

    USER = "user"
    EMPLOYEE = "employee"
    ATTENDANCE = "attendance"
    LEAVE = "leave"
    NOTIFICATION = "notification"
    COMPLIANCE = "compliance"
    PAYROLL = "payroll"
    SYSTEM = "system"


# Role hierarchy (higher number = more access)
ROLE_HIERARCHY = {
    Role.HR_ADMIN: 100,
    Role.HR_MANAGER: 75,
    Role.MANAGER: 50,
    Role.EMPLOYEE: 10,
}

# Domains each role can access
ROLE_DOMAIN_ACCESS = {
    Role.HR_ADMIN: [
        AuditDomain.USER,
        AuditDomain.EMPLOYEE,
        AuditDomain.ATTENDANCE,
        AuditDomain.LEAVE,
        AuditDomain.NOTIFICATION,
        AuditDomain.COMPLIANCE,
        AuditDomain.PAYROLL,
        AuditDomain.SYSTEM,
    ],
    Role.HR_MANAGER: [
        AuditDomain.EMPLOYEE,
        AuditDomain.ATTENDANCE,
        AuditDomain.LEAVE,
        AuditDomain.NOTIFICATION,
    ],
    Role.MANAGER: [
        AuditDomain.ATTENDANCE,
        AuditDomain.LEAVE,
    ],
    Role.EMPLOYEE: [],
}

# Sensitive log types that require special permissions
SENSITIVE_LOG_TYPES = [
    "salary_updated",
    "salary_increment",
    "role_assignment",
    "permission_change",
    "employee_terminated",
    "payroll",
]


def get_role_from_string(role_str: str) -> Optional[Role]:
    """
    Convert a role string to Role enum.

    Args:
        role_str: Role as string

    Returns:
        Role enum or None if invalid
    """
    role_map = {
        "HR_Admin": Role.HR_ADMIN,
        "HR_ADMIN": Role.HR_ADMIN,
        "hr_admin": Role.HR_ADMIN,
        "HR_Manager": Role.HR_MANAGER,
        "HR_MANAGER": Role.HR_MANAGER,
        "hr_manager": Role.HR_MANAGER,
        "manager": Role.MANAGER,
        "Manager": Role.MANAGER,
        "MANAGER": Role.MANAGER,
        "employee": Role.EMPLOYEE,
        "Employee": Role.EMPLOYEE,
        "EMPLOYEE": Role.EMPLOYEE,
    }
    return role_map.get(role_str)


def get_role_level(role: Role) -> int:
    """
    Get the hierarchy level of a role.

    Args:
        role: User role

    Returns:
        Hierarchy level (higher = more access)
    """
    return ROLE_HIERARCHY.get(role, 0)


def can_view_audit_logs(viewer_role: Role) -> bool:
    """
    Check if a role can view any audit logs.

    Args:
        viewer_role: The role of the user trying to view logs

    Returns:
        True if the role can view audit logs
    """
    # Employees cannot view audit logs
    return viewer_role != Role.EMPLOYEE


def can_view_domain(viewer_role: Role, domain: AuditDomain) -> bool:
    """
    Check if a role can view audit logs for a specific domain.

    Args:
        viewer_role: The role of the user trying to view logs
        domain: The audit domain to access

    Returns:
        True if the role can view the domain
    """
    allowed_domains = ROLE_DOMAIN_ACCESS.get(viewer_role, [])
    return domain in allowed_domains


def can_view_log_type(viewer_role: Role, log_type: str) -> bool:
    """
    Check if a role can view a specific log type.

    Args:
        viewer_role: The role of the user trying to view logs
        log_type: The type of audit log

    Returns:
        True if the role can view the log type
    """
    # HR_Admin can view everything
    if viewer_role == Role.HR_ADMIN:
        return True

    # Other roles cannot view sensitive log types
    if log_type in SENSITIVE_LOG_TYPES:
        return False

    return True


def can_view_user_logs(
    viewer_role: Role,
    viewer_user_id: str,
    target_user_id: str,
    target_role: Optional[Role] = None,
) -> bool:
    """
    Check if a user can view audit logs for another user.

    Args:
        viewer_role: Role of the user trying to view logs
        viewer_user_id: ID of the user trying to view logs
        target_user_id: ID of the user whose logs are being viewed
        target_role: Role of the target user (if known)

    Returns:
        True if the viewer can see the target's logs
    """
    # HR_Admin can see everyone's logs
    if viewer_role == Role.HR_ADMIN:
        return True

    # Employees cannot view audit logs
    if viewer_role == Role.EMPLOYEE:
        return False

    # Users can always see their own logs
    if viewer_user_id == target_user_id:
        return True

    # If we don't know the target's role, be conservative
    if target_role is None:
        # HR_Manager can view if they have general access
        return viewer_role == Role.HR_MANAGER

    # HR_Manager can see manager and employee logs, but not other HR_Managers
    if viewer_role == Role.HR_MANAGER:
        if target_role == Role.HR_MANAGER:
            return False  # Cannot see other HR_Managers
        if target_role == Role.HR_ADMIN:
            return False  # Cannot see HR_Admin
        return True  # Can see managers and employees

    # Manager can only see employee logs
    if viewer_role == Role.MANAGER:
        return target_role == Role.EMPLOYEE

    return False


def can_view_self_logs(viewer_role: Role) -> bool:
    """
    Check if a role can view their own audit logs.

    Args:
        viewer_role: The role of the user

    Returns:
        True if the role can view their own logs
    """
    # HR_Managers cannot see audit events of themselves per spec
    if viewer_role == Role.HR_MANAGER:
        return False

    # HR_Admin can see everything including own logs
    if viewer_role == Role.HR_ADMIN:
        return True

    # Managers and employees don't have use cases to see audit events
    return False


def filter_audit_logs_for_role(
    logs: list[dict],
    viewer_role: Role,
    viewer_user_id: str,
) -> list[dict]:
    """
    Filter a list of audit logs based on the viewer's role.

    Args:
        logs: List of audit log dictionaries
        viewer_role: Role of the user viewing the logs
        viewer_user_id: ID of the user viewing the logs

    Returns:
        Filtered list of audit logs the user can see
    """
    if viewer_role == Role.HR_ADMIN:
        return logs  # HR_Admin sees everything

    if viewer_role == Role.EMPLOYEE:
        return []  # Employees see nothing

    filtered = []
    for log in logs:
        # Get the domain from the log
        entity_type = log.get("entity_type", "unknown")
        domain = _entity_type_to_domain(entity_type)

        # Check domain access
        if domain and not can_view_domain(viewer_role, domain):
            continue

        # Check log type access
        log_type = log.get("log_type", "")
        if not can_view_log_type(viewer_role, log_type):
            continue

        # For HR_Manager, filter out other HR_Manager logs
        if viewer_role == Role.HR_MANAGER:
            actor_role = log.get("actor_role", "")
            if actor_role == Role.HR_MANAGER.value:
                continue

        filtered.append(log)

    return filtered


def _entity_type_to_domain(entity_type: str) -> Optional[AuditDomain]:
    """
    Map entity type to audit domain.

    Args:
        entity_type: The entity type from the audit log

    Returns:
        Corresponding AuditDomain or None
    """
    mapping = {
        "user": AuditDomain.USER,
        "employee": AuditDomain.EMPLOYEE,
        "attendance": AuditDomain.ATTENDANCE,
        "leave": AuditDomain.LEAVE,
        "notification": AuditDomain.NOTIFICATION,
        "compliance": AuditDomain.COMPLIANCE,
        "payroll": AuditDomain.PAYROLL,
        "system": AuditDomain.SYSTEM,
    }
    return mapping.get(entity_type.lower())


def get_allowed_service_names(viewer_role: Role) -> list[str]:
    """
    Get the list of service names a role can view logs from.

    Args:
        viewer_role: Role of the user

    Returns:
        List of allowed service names
    """
    if viewer_role == Role.HR_ADMIN:
        return [
            "user-management-service",
            "employee-management-service",
            "attendance-management-service",
            "leave-management-service",
            "notification-service",
            "audit-service",
            "compliance-service",
        ]

    if viewer_role == Role.HR_MANAGER:
        return [
            "employee-management-service",
            "attendance-management-service",
            "leave-management-service",
            "notification-service",
        ]

    if viewer_role == Role.MANAGER:
        return [
            "attendance-management-service",
            "leave-management-service",
        ]

    return []


def build_role_filter_query(viewer_role: Role, viewer_user_id: str) -> dict:
    """
    Build a filter dictionary for database queries based on role.

    Args:
        viewer_role: Role of the user
        viewer_user_id: ID of the user

    Returns:
        Dictionary with filter conditions
    """
    filters = {
        "allowed": can_view_audit_logs(viewer_role),
        "service_names": get_allowed_service_names(viewer_role),
        "exclude_sensitive": viewer_role != Role.HR_ADMIN,
        "exclude_self": viewer_role == Role.HR_MANAGER,
        "user_id": viewer_user_id if viewer_role == Role.HR_MANAGER else None,
    }

    return filters


class RBACChecker:
    """
    RBAC checker class for audit service access control.
    Provides methods to check various access permissions.
    """

    def __init__(self, user_id: str, role: Role):
        self.user_id = user_id
        self.role = role

    @classmethod
    def from_jwt_claims(cls, claims: dict) -> "RBACChecker":
        """
        Create an RBACChecker from JWT claims.

        Args:
            claims: JWT token claims

        Returns:
            RBACChecker instance
        """
        user_id = claims.get("sub", claims.get("user_id", "unknown"))

        # Extract role from claims (adjust based on your JWT structure)
        groups = claims.get("groups", [])
        role_str = None

        for group in groups:
            if group in ["HR_Admin", "HR_Manager", "manager", "employee"]:
                role_str = group
                break

        if role_str is None:
            role_str = claims.get("role", "employee")

        role = get_role_from_string(role_str) or Role.EMPLOYEE

        return cls(user_id, role)

    def can_view_logs(self) -> bool:
        """Check if the user can view any audit logs."""
        return can_view_audit_logs(self.role)

    def can_view_domain(self, domain: AuditDomain) -> bool:
        """Check if the user can view a specific domain."""
        return can_view_domain(self.role, domain)

    def can_view_user(
        self,
        target_user_id: str,
        target_role: Optional[Role] = None,
    ) -> bool:
        """Check if the user can view another user's logs."""
        return can_view_user_logs(
            self.role,
            self.user_id,
            target_user_id,
            target_role,
        )

    def filter_logs(self, logs: list[dict]) -> list[dict]:
        """Filter a list of logs based on the user's permissions."""
        return filter_audit_logs_for_role(logs, self.role, self.user_id)

    def get_query_filters(self) -> dict:
        """Get database query filters for the user."""
        return build_role_filter_query(self.role, self.user_id)
