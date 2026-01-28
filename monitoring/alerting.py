"""
Alerting System - Sends alerts for failures and anomalies.
"""

import logging
from typing import Dict, List, Any
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertManager:
    """
    Manages alerts and notifications:
    - Slack notifications
    - Email alerts
    - Alert history and deduplication
    - Alert routing by severity
    """

    def __init__(self, slack_webhook_url: str = None,
                 email_recipients: List[str] = None):
        """
        Args:
            slack_webhook_url: Slack webhook URL for notifications
            email_recipients: List of email addresses for alerts
        """
        self.slack_webhook_url = slack_webhook_url
        self.email_recipients = email_recipients or []
        self.alert_history = []
        self.alert_dedup_window = 3600  # seconds (1 hour)

    def send_alert(self, title: str,
                  message: str,
                  level: AlertLevel = AlertLevel.WARNING,
                  metadata: Dict = None) -> bool:
        """
        Send alert via configured channels.
        
        Args:
            title: Alert title
            message: Alert message
            level: Alert severity level
            metadata: Additional context data
        
        Returns:
            True if alert sent successfully
        """
        alert = {
            "timestamp": datetime.utcnow().isoformat(),
            "title": title,
            "message": message,
            "level": level.value,
            "metadata": metadata or {},
            "sent": False
        }

        # Check deduplication
        if self._is_duplicate_alert(title):
            logger.info(f"Alert deduplicated: {title}")
            return False

        try:
            # Send to Slack
            if self.slack_webhook_url:
                self._send_slack_notification(title, message, level, metadata)
                alert["slack_sent"] = True

            # Send via email
            if self.email_recipients:
                self._send_email_notification(title, message, level, metadata)
                alert["email_sent"] = True

            alert["sent"] = True

        except Exception as e:
            logger.error(f"Error sending alert: {e}")
            alert["error"] = str(e)

        self.alert_history.append(alert)
        return alert["sent"]

    def _send_slack_notification(self, title: str,
                                message: str,
                                level: AlertLevel,
                                metadata: Dict = None) -> bool:
        """
        Send notification to Slack.
        
        Args:
            title: Alert title
            message: Alert message
            level: Alert severity
            metadata: Additional context
        
        Returns:
            Success status
        """
        try:
            # TODO: Implement Slack webhook call
            # import requests
            # color_map = {
            #     AlertLevel.INFO: "#36a64f",
            #     AlertLevel.WARNING: "#ff9900",
            #     AlertLevel.ERROR: "#ff0000",
            #     AlertLevel.CRITICAL: "#990000"
            # }
            # payload = {
            #     "attachments": [{
            #         "title": title,
            #         "text": message,
            #         "color": color_map.get(level),
            #         "fields": [{"title": k, "value": str(v)} for k, v in (metadata or {}).items()]
            #     }]
            # }
            # requests.post(self.slack_webhook_url, json=payload)
            return True

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False

    def _send_email_notification(self, title: str,
                                message: str,
                                level: AlertLevel,
                                metadata: Dict = None) -> bool:
        """
        Send notification via email.
        
        Args:
            title: Alert title
            message: Alert message
            level: Alert severity
            metadata: Additional context
        
        Returns:
            Success status
        """
        try:
            # TODO: Implement email notification
            # import smtplib
            # from email.mime.text import MIMEText
            # email_body = f"{title}\n\n{message}\n\n{metadata}"
            # msg = MIMEText(email_body)
            # msg['Subject'] = f"[{level.value.upper()}] {title}"
            # msg['From'] = sender
            # msg['To'] = ', '.join(self.email_recipients)
            # smtp_connection.send_message(msg)
            return True

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False

    def _is_duplicate_alert(self, alert_title: str) -> bool:
        """
        Check if similar alert was recently sent.
        
        Args:
            alert_title: Title of alert to check
        
        Returns:
            True if duplicate within dedup window
        """
        current_time = datetime.utcnow()

        for alert in reversed(self.alert_history[-100:]):  # Check last 100 alerts
            if alert["title"] == alert_title:
                alert_time = datetime.fromisoformat(alert["timestamp"])
                time_diff = (current_time - alert_time).total_seconds()

                if time_diff < self.alert_dedup_window:
                    return True

        return False

    def alert_extraction_failure(self, source: str, error: str) -> bool:
        """Alert on extraction failure."""
        return self.send_alert(
            title=f"Extraction Failed: {source}",
            message=f"Data extraction failed for source {source}",
            level=AlertLevel.ERROR,
            metadata={"source": source, "error": error}
        )

    def alert_quality_check_failure(self, check_name: str, details: str) -> bool:
        """Alert on data quality check failure."""
        return self.send_alert(
            title=f"Quality Check Failed: {check_name}",
            message=f"Data quality check failed: {check_name}",
            level=AlertLevel.ERROR,
            metadata={"check": check_name, "details": details}
        )

    def alert_sla_breach(self, dag_id: str, duration_minutes: float,
                        sla_minutes: float) -> bool:
        """Alert on DAG SLA breach."""
        return self.send_alert(
            title=f"SLA Breach: {dag_id}",
            message=f"DAG {dag_id} took {duration_minutes}m, SLA is {sla_minutes}m",
            level=AlertLevel.WARNING,
            metadata={
                "dag": dag_id,
                "actual_minutes": duration_minutes,
                "sla_minutes": sla_minutes
            }
        )

    def alert_data_anomaly(self, anomaly_type: str, details: str) -> bool:
        """Alert on detected data anomaly."""
        return self.send_alert(
            title=f"Data Anomaly: {anomaly_type}",
            message=f"Anomaly detected: {anomaly_type}",
            level=AlertLevel.WARNING,
            metadata={"type": anomaly_type, "details": details}
        )

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alerts."""
        alerts_by_level = {}
        for alert in self.alert_history:
            level = alert["level"]
            alerts_by_level[level] = alerts_by_level.get(level, 0) + 1

        return {
            "total_alerts": len(self.alert_history),
            "alerts_by_level": alerts_by_level,
            "recent_alerts": self.alert_history[-10:],
            "timestamp": datetime.utcnow().isoformat()
        }
