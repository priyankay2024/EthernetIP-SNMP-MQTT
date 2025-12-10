from database import db
from datetime import datetime

class EthernetIPConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, default="Default")
    ip_address = db.Column(db.String(45), nullable=False)
    slot = db.Column(db.Integer, default=0)
    timeout = db.Column(db.Float, default=5.0)
    hwid = db.Column(db.String(100), nullable=True)
    polling_interval = db.Column(db.Integer, default=1000)
    description = db.Column(db.String(255), nullable=True)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SNMPConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, default="Default")
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, default=161)
    community = db.Column(db.String(100), default="public")
    version = db.Column(db.String(10), default="v2c")
    hwid = db.Column(db.String(100), nullable=True)
    polling_interval = db.Column(db.Integer, default=5000)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class MQTTConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, default="Default")
    broker = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, default=1883)
    username = db.Column(db.String(100), nullable=True)
    password = db.Column(db.String(255), nullable=True)
    topic_prefix = db.Column(db.String(100), default="bridge")
    publish_format = db.Column(db.String(20), default="json")  # 'json' or 'string'
    use_tls = db.Column(db.Boolean, default=False)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class EthernetIPTag(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    config_id = db.Column(db.Integer, db.ForeignKey('ethernet_ip_config.id', ondelete='CASCADE'), nullable=False)
    tag_name = db.Column(db.String(255), nullable=False)
    data_type = db.Column(db.String(50), nullable=True)
    description = db.Column(db.String(500), nullable=True)
    poll_rate = db.Column(db.Integer, default=1000)
    enabled = db.Column(db.Boolean, default=True)
    last_value = db.Column(db.String(255), nullable=True)
    last_read = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    config = db.relationship('EthernetIPConfig', backref=db.backref('tags', lazy=True, cascade='all, delete-orphan'))

class SNMPObject(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    config_id = db.Column(db.Integer, db.ForeignKey('snmp_config.id', ondelete='CASCADE'), nullable=False)
    oid = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=True)
    description = db.Column(db.String(500), nullable=True)
    data_type = db.Column(db.String(50), nullable=True)  # MIB Syntax: INTEGER, STRING, Counter32, etc.
    access = db.Column(db.String(20), nullable=True)  # read-only, read-write, etc.
    status = db.Column(db.String(20), nullable=True)  # current, deprecated, etc.
    poll_rate = db.Column(db.Integer, default=5000)
    enabled = db.Column(db.Boolean, default=True)
    last_value = db.Column(db.String(255), nullable=True)
    last_read = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    config = db.relationship('SNMPConfig', backref=db.backref('objects', lazy=True, cascade='all, delete-orphan'))


class DataLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_type = db.Column(db.String(20), nullable=False)
    source_id = db.Column(db.Integer, nullable=False)
    source_name = db.Column(db.String(255), nullable=False)
    value = db.Column(db.String(500), nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    __table_args__ = (
        db.Index('idx_source_time', 'source_type', 'source_id', 'timestamp'),
    )


class TagMapping(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    source_type = db.Column(db.String(20), nullable=False)
    source_id = db.Column(db.Integer, nullable=False)
    mqtt_topic = db.Column(db.String(255), nullable=False)
    transform_expression = db.Column(db.String(500), nullable=True)
    publish_on_change = db.Column(db.Boolean, default=True)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class PollingSchedule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    source_type = db.Column(db.String(20), nullable=False)
    poll_interval = db.Column(db.Integer, default=5000)
    mqtt_config_id = db.Column(db.Integer, db.ForeignKey('mqtt_config.id'), nullable=True)
    enabled = db.Column(db.Boolean, default=True)
    last_run = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    mqtt_config = db.relationship('MQTTConfig', backref=db.backref('schedules', lazy=True))


class MQTTSubscription(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    mqtt_config_id = db.Column(db.Integer, db.ForeignKey('mqtt_config.id'), nullable=False)
    topic = db.Column(db.String(255), nullable=False)
    target_type = db.Column(db.String(20), nullable=False)
    target_id = db.Column(db.Integer, nullable=False)
    enabled = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    mqtt_config = db.relationship('MQTTConfig', backref=db.backref('subscriptions', lazy=True))
