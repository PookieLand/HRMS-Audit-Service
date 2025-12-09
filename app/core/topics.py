"""
Kafka Topic Definitions for Audit Service.

The Audit Service subscribes to audit event topics from all other services
to create a centralized audit trail. Topics follow the pattern: <domain>-<event-type>
"""


class KafkaTopics:
    """
    Central registry of all Kafka topics consumed by the Audit Service.
    Topics are organized by source service.
    """

    # ==========================================
    # User Management Service Audit Topics
    # ==========================================

    # User lifecycle audit events
    AUDIT_USER_ACTION = "audit-user-action"

    # User events to audit
    USER_CREATED = "user-created"
    USER_UPDATED = "user-updated"
    USER_DELETED = "user-deleted"
    USER_SUSPENDED = "user-suspended"
    USER_ACTIVATED = "user-activated"
    USER_ROLE_CHANGED = "user-role-changed"

    # Onboarding events to audit
    USER_ONBOARDING_INITIATED = "user-onboarding-initiated"
    USER_ONBOARDING_ASGARDEO_CREATED = "user-onboarding-asgardeo-created"
    USER_ONBOARDING_EMPLOYEE_CREATED = "user-onboarding-employee-created"
    USER_ONBOARDING_COMPLETED = "user-onboarding-completed"
    USER_ONBOARDING_FAILED = "user-onboarding-failed"

    # ==========================================
    # Employee Management Service Audit Topics
    # ==========================================

    # Employee audit events
    AUDIT_EMPLOYEE_ACTION = "audit-employee-action"

    # Employee lifecycle events to audit
    EMPLOYEE_CREATED = "employee-created"
    EMPLOYEE_UPDATED = "employee-updated"
    EMPLOYEE_DELETED = "employee-deleted"
    EMPLOYEE_TERMINATED = "employee-terminated"
    EMPLOYEE_PROMOTED = "employee-promoted"
    EMPLOYEE_TRANSFERRED = "employee-transferred"

    # Employment status events to audit
    EMPLOYEE_PROBATION_STARTED = "employee-probation-started"
    EMPLOYEE_PROBATION_COMPLETED = "employee-probation-completed"
    EMPLOYEE_CONTRACT_STARTED = "employee-contract-started"
    EMPLOYEE_CONTRACT_RENEWED = "employee-contract-renewed"
    EMPLOYEE_CONTRACT_ENDED = "employee-contract-ended"

    # Salary events to audit
    EMPLOYEE_SALARY_UPDATED = "employee-salary-updated"
    EMPLOYEE_SALARY_INCREMENT = "employee-salary-increment"

    # Department/Team changes to audit
    EMPLOYEE_DEPARTMENT_CHANGED = "employee-department-changed"
    EMPLOYEE_TEAM_CHANGED = "employee-team-changed"
    EMPLOYEE_MANAGER_CHANGED = "employee-manager-changed"

    # ==========================================
    # Attendance Management Service Audit Topics
    # ==========================================

    # Attendance audit events
    AUDIT_ATTENDANCE_ACTION = "audit-attendance-action"

    # Attendance events to audit
    ATTENDANCE_CHECKIN = "attendance-checkin"
    ATTENDANCE_CHECKOUT = "attendance-checkout"
    ATTENDANCE_UPDATED = "attendance-updated"
    ATTENDANCE_DELETED = "attendance-deleted"
    ATTENDANCE_LATE = "attendance-late"
    ATTENDANCE_EARLY_DEPARTURE = "attendance-early-departure"
    ATTENDANCE_OVERTIME = "attendance-overtime"
    ATTENDANCE_SHORT_LEAVE = "attendance-short-leave"
    ATTENDANCE_ABSENT = "attendance-absent"

    # ==========================================
    # Leave Management Service Audit Topics
    # ==========================================

    # Leave audit events
    AUDIT_LEAVE_ACTION = "audit-leave-action"

    # Leave events to audit
    LEAVE_REQUESTED = "leave-requested"
    LEAVE_APPROVED = "leave-approved"
    LEAVE_REJECTED = "leave-rejected"
    LEAVE_CANCELLED = "leave-cancelled"
    LEAVE_MODIFIED = "leave-modified"
    LEAVE_REVOKED = "leave-revoked"
    LEAVE_STARTED = "leave-started"
    LEAVE_ENDED = "leave-ended"
    LEAVE_EXTENDED = "leave-extended"

    # Leave balance events to audit
    LEAVE_BALANCE_UPDATED = "leave-balance-updated"
    LEAVE_BALANCE_RESET = "leave-balance-reset"
    LEAVE_ACCRUED = "leave-accrued"

    # ==========================================
    # Notification Service Audit Topics
    # ==========================================

    # Notification audit events
    AUDIT_NOTIFICATION_ACTION = "audit-notification-action"

    # Notification events to audit
    NOTIFICATION_SENT = "notification-sent"
    NOTIFICATION_FAILED = "notification-failed"
    NOTIFICATION_DELIVERED = "notification-delivered"

    # ==========================================
    # Compliance Service Audit Topics
    # ==========================================

    # Compliance audit events
    AUDIT_COMPLIANCE_ACTION = "audit-compliance-action"

    # ==========================================
    # Helper Methods
    # ==========================================

    @classmethod
    def all_subscribed_topics(cls) -> list[str]:
        """Return list of all topics the audit service subscribes to."""
        return (
            cls.user_audit_topics()
            + cls.employee_audit_topics()
            + cls.attendance_audit_topics()
            + cls.leave_audit_topics()
            + cls.notification_audit_topics()
            + cls.compliance_audit_topics()
        )

    @classmethod
    def user_audit_topics(cls) -> list[str]:
        """Return list of user-related audit topics."""
        return [
            cls.AUDIT_USER_ACTION,
            cls.USER_CREATED,
            cls.USER_UPDATED,
            cls.USER_DELETED,
            cls.USER_SUSPENDED,
            cls.USER_ACTIVATED,
            cls.USER_ROLE_CHANGED,
            cls.USER_ONBOARDING_INITIATED,
            cls.USER_ONBOARDING_ASGARDEO_CREATED,
            cls.USER_ONBOARDING_EMPLOYEE_CREATED,
            cls.USER_ONBOARDING_COMPLETED,
            cls.USER_ONBOARDING_FAILED,
        ]

    @classmethod
    def employee_audit_topics(cls) -> list[str]:
        """Return list of employee-related audit topics."""
        return [
            cls.AUDIT_EMPLOYEE_ACTION,
            cls.EMPLOYEE_CREATED,
            cls.EMPLOYEE_UPDATED,
            cls.EMPLOYEE_DELETED,
            cls.EMPLOYEE_TERMINATED,
            cls.EMPLOYEE_PROMOTED,
            cls.EMPLOYEE_TRANSFERRED,
            cls.EMPLOYEE_PROBATION_STARTED,
            cls.EMPLOYEE_PROBATION_COMPLETED,
            cls.EMPLOYEE_CONTRACT_STARTED,
            cls.EMPLOYEE_CONTRACT_RENEWED,
            cls.EMPLOYEE_CONTRACT_ENDED,
            cls.EMPLOYEE_SALARY_UPDATED,
            cls.EMPLOYEE_SALARY_INCREMENT,
            cls.EMPLOYEE_DEPARTMENT_CHANGED,
            cls.EMPLOYEE_TEAM_CHANGED,
            cls.EMPLOYEE_MANAGER_CHANGED,
        ]

    @classmethod
    def attendance_audit_topics(cls) -> list[str]:
        """Return list of attendance-related audit topics."""
        return [
            cls.AUDIT_ATTENDANCE_ACTION,
            cls.ATTENDANCE_CHECKIN,
            cls.ATTENDANCE_CHECKOUT,
            cls.ATTENDANCE_UPDATED,
            cls.ATTENDANCE_DELETED,
            cls.ATTENDANCE_LATE,
            cls.ATTENDANCE_EARLY_DEPARTURE,
            cls.ATTENDANCE_OVERTIME,
            cls.ATTENDANCE_SHORT_LEAVE,
            cls.ATTENDANCE_ABSENT,
        ]

    @classmethod
    def leave_audit_topics(cls) -> list[str]:
        """Return list of leave-related audit topics."""
        return [
            cls.AUDIT_LEAVE_ACTION,
            cls.LEAVE_REQUESTED,
            cls.LEAVE_APPROVED,
            cls.LEAVE_REJECTED,
            cls.LEAVE_CANCELLED,
            cls.LEAVE_MODIFIED,
            cls.LEAVE_REVOKED,
            cls.LEAVE_STARTED,
            cls.LEAVE_ENDED,
            cls.LEAVE_EXTENDED,
            cls.LEAVE_BALANCE_UPDATED,
            cls.LEAVE_BALANCE_RESET,
            cls.LEAVE_ACCRUED,
        ]

    @classmethod
    def notification_audit_topics(cls) -> list[str]:
        """Return list of notification-related audit topics."""
        return [
            cls.AUDIT_NOTIFICATION_ACTION,
            cls.NOTIFICATION_SENT,
            cls.NOTIFICATION_FAILED,
            cls.NOTIFICATION_DELIVERED,
        ]

    @classmethod
    def compliance_audit_topics(cls) -> list[str]:
        """Return list of compliance-related audit topics."""
        return [
            cls.AUDIT_COMPLIANCE_ACTION,
        ]

    @classmethod
    def sensitive_topics(cls) -> list[str]:
        """
        Return list of topics containing sensitive data.
        These require special handling and access control.
        """
        return [
            cls.EMPLOYEE_SALARY_UPDATED,
            cls.EMPLOYEE_SALARY_INCREMENT,
            cls.USER_ROLE_CHANGED,
            cls.EMPLOYEE_TERMINATED,
        ]

    @classmethod
    def get_topic_domain(cls, topic: str) -> str:
        """
        Extract the domain from a topic name.

        Args:
            topic: Kafka topic name

        Returns:
            Domain name (user, employee, attendance, leave, notification, compliance)
        """
        if topic.startswith("user-") or topic.startswith("audit-user"):
            return "user"
        elif topic.startswith("employee-") or topic.startswith("audit-employee"):
            return "employee"
        elif topic.startswith("attendance-") or topic.startswith("audit-attendance"):
            return "attendance"
        elif topic.startswith("leave-") or topic.startswith("audit-leave"):
            return "leave"
        elif topic.startswith("notification-") or topic.startswith(
            "audit-notification"
        ):
            return "notification"
        elif topic.startswith("compliance-") or topic.startswith("audit-compliance"):
            return "compliance"
        else:
            return "unknown"

    @classmethod
    def get_topics_for_role(cls, role: str) -> list[str]:
        """
        Get the topics a role is allowed to view audit logs for.

        Args:
            role: User role (HR_Admin, HR_Manager, manager, employee)

        Returns:
            List of topics the role can access
        """
        if role == "HR_Admin":
            # HR_Admin can see everything
            return cls.all_subscribed_topics()

        elif role == "HR_Manager":
            # HR_Manager can see employee, attendance, leave events
            # but not other HR_Manager actions
            return (
                cls.employee_audit_topics()
                + cls.attendance_audit_topics()
                + cls.leave_audit_topics()
            )

        elif role == "manager":
            # Manager can see employee attendance and leave events
            return cls.attendance_audit_topics() + cls.leave_audit_topics()

        elif role == "employee":
            # Employees don't see audit events (they only generate them)
            return []

        else:
            return []
