import logging
from datetime import datetime, timedelta
from app import db
from models import DataLog

logger = logging.getLogger(__name__)

class DataLoggingService:
    def __init__(self):
        pass
    
    def log_value(self, source_type, source_id, source_name, value):
        try:
            log_entry = DataLog(
                source_type=source_type,
                source_id=source_id,
                source_name=source_name,
                value=str(value) if value is not None else None,
                timestamp=datetime.utcnow()
            )
            db.session.add(log_entry)
            db.session.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to log value: {str(e)}")
            db.session.rollback()
            return False
    
    def get_history(self, source_type, source_id, hours=24, limit=1000):
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            logs = DataLog.query.filter(
                DataLog.source_type == source_type,
                DataLog.source_id == source_id,
                DataLog.timestamp >= since
            ).order_by(DataLog.timestamp.desc()).limit(limit).all()
            
            return [
                {
                    'timestamp': log.timestamp.isoformat(),
                    'value': log.value,
                    'source_name': log.source_name
                }
                for log in reversed(logs)
            ]
        except Exception as e:
            logger.error(f"Failed to get history: {str(e)}")
            return []
    
    def get_recent_logs(self, limit=100):
        try:
            logs = DataLog.query.order_by(
                DataLog.timestamp.desc()
            ).limit(limit).all()
            
            return [
                {
                    'id': log.id,
                    'source_type': log.source_type,
                    'source_id': log.source_id,
                    'source_name': log.source_name,
                    'value': log.value,
                    'timestamp': log.timestamp.isoformat()
                }
                for log in logs
            ]
        except Exception as e:
            logger.error(f"Failed to get recent logs: {str(e)}")
            return []
    
    def cleanup_old_logs(self, days=7):
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            deleted = DataLog.query.filter(DataLog.timestamp < cutoff).delete()
            db.session.commit()
            logger.info(f"Cleaned up {deleted} old log entries")
            return deleted
        except Exception as e:
            logger.error(f"Failed to cleanup logs: {str(e)}")
            db.session.rollback()
            return 0
    
    def get_chart_data(self, source_type, source_id, hours=24):
        history = self.get_history(source_type, source_id, hours=hours)
        
        labels = []
        values = []
        
        for entry in history:
            labels.append(entry['timestamp'])
            try:
                values.append(float(entry['value']))
            except (ValueError, TypeError):
                values.append(None)
        
        return {
            'labels': labels,
            'values': values
        }
