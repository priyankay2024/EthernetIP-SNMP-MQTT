"""
Polling Service - Background data collection and MQTT publishing
Continuously reads EthernetIP tags and SNMP objects, publishes to MQTT
Uses asyncio and concurrent threading for parallel device polling
"""
import asyncio
import threading
import time
import logging
from datetime import datetime
from flask import Flask
import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial

logger = logging.getLogger(__name__)

class PollingService:
    def __init__(self, app, db, ethernetip_service, snmp_service, mqtt_service, data_logging_service):
        self.app = app  # Store Flask app for context
        self.db = db
        self.eip_service = ethernetip_service
        self.snmp_service = snmp_service
        self.mqtt_service = mqtt_service
        self.data_logging_service = data_logging_service
        
        # Threading infrastructure
        self._eip_thread = None
        self._snmp_thread = None
        self._reconnect_thread = None
        self._stop_event = threading.Event()
        self._running = False
        
        # Thread pools for parallel device polling
        self._eip_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="EIP-Worker")
        self._snmp_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="SNMP-Worker")
        
        # Tracking dictionaries (thread-safe with locks)
        self._last_poll_time = {}  # Track last poll time per device
        self._last_reconnect_attempt = {}  # Track last reconnection attempt
        self._reconnect_interval = 10  # Reconnect check interval in seconds
        self._last_log_time = {}  # Track last log time per device
        self._log_interval = 30  # Log detailed info every 30 seconds
        self._lock = threading.Lock()  # Thread safety for shared data
        
        logger.info("Polling Service initialized with parallel threading")
        print("=== Polling Service Initialized (Multi-threaded) ===")
    
    def start(self):
        """Start all background polling threads"""
        if self._running:
            logger.warning("Polling service already running")
            return
        
        self._stop_event.clear()
        self._running = True
        
        # Start separate threads for each protocol
        self._eip_thread = threading.Thread(target=self._ethernetip_loop, daemon=True, name="EIP-Main")
        self._snmp_thread = threading.Thread(target=self._snmp_loop, daemon=True, name="SNMP-Main")
        self._reconnect_thread = threading.Thread(target=self._reconnect_loop, daemon=True, name="Reconnect")
        
        self._eip_thread.start()
        self._snmp_thread.start()
        self._reconnect_thread.start()
        
        logger.info("Polling service started - 3 main threads + 2 worker pools")
    
    def stop(self):
        """Stop all background threads and cleanup"""
        if not self._running:
            return
        
        self._stop_event.set()
        self._running = False
        
        # Wait for threads to finish
        threads = [self._eip_thread, self._snmp_thread, self._reconnect_thread]
        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=5)
        
        # Shutdown executor pools
        self._eip_executor.shutdown(wait=True, cancel_futures=True)
        self._snmp_executor.shutdown(wait=True, cancel_futures=True)
        
        logger.info("Polling service stopped - all threads terminated")
        print("=== Polling Service Stopped ===")
    
    def _ethernetip_loop(self):
        """Main EthernetIP polling loop - runs in dedicated thread"""
        logger.info("EthernetIP polling thread started")
        
        while not self._stop_event.is_set():
            try:
                with self.app.app_context():
                    from models import EthernetIPConfig, EthernetIPTag, MQTTConfig
                    
                    # Get all enabled devices
                    configs = self.db.session.query(EthernetIPConfig).filter_by(enabled=True).all()
                    
                    if not configs:
                        self._stop_event.wait(5.0)
                        continue
                    
                    # Submit each device to thread pool for parallel polling
                    futures = []
                    for config in configs:
                        future = self._eip_executor.submit(
                            self._poll_single_ethernetip_device,
                            config.id
                        )
                        futures.append(future)
                    
                    # Wait for all devices to complete (with timeout)
                    for future in futures:
                        try:
                            future.result(timeout=10)
                        except Exception as e:
                            logger.error(f"EIP device polling error: {str(e)}")
                
                # Short sleep between poll cycles
                self._stop_event.wait(0.5)
                
            except Exception as e:
                logger.error(f"Error in EthernetIP loop: {str(e)}", exc_info=True)
                self._stop_event.wait(5.0)
    
    def _snmp_loop(self):
        """Main SNMP polling loop - runs in dedicated thread"""
        logger.info("SNMP polling thread started")
        
        while not self._stop_event.is_set():
            try:
                with self.app.app_context():
                    from models import SNMPConfig, SNMPObject, MQTTConfig
                    
                    # Get all enabled SNMP devices
                    configs = self.db.session.query(SNMPConfig).filter_by(enabled=True).all()
                    
                    if not configs:
                        self._stop_event.wait(5.0)
                        continue
                    
                    # Submit each device to thread pool for parallel polling
                    futures = []
                    for config in configs:
                        future = self._snmp_executor.submit(
                            self._poll_single_snmp_device,
                            config.id
                        )
                        futures.append(future)
                    
                    # Wait for all devices to complete (with timeout)
                    for future in futures:
                        try:
                            future.result(timeout=10)
                        except Exception as e:
                            logger.error(f"SNMP device polling error: {str(e)}")
                
                # Short sleep between poll cycles
                self._stop_event.wait(0.5)
                
            except Exception as e:
                logger.error(f"Error in SNMP loop: {str(e)}", exc_info=True)
                self._stop_event.wait(5.0)
    
    def _reconnect_loop(self):
        """Reconnection loop - runs in dedicated thread"""
        logger.info("Reconnection thread started")
        
        while not self._stop_event.is_set():
            try:
                with self.app.app_context():
                    from models import EthernetIPConfig, SNMPConfig, MQTTConfig
                    self._reconnect_offline_devices(EthernetIPConfig, SNMPConfig, MQTTConfig)
                
                # Check every 10 seconds
                self._stop_event.wait(10.0)
                
            except Exception as e:
                logger.error(f"Error in reconnection loop: {str(e)}", exc_info=True)
                self._stop_event.wait(10.0)
    
    def _poll_single_ethernetip_device(self, config_id):
        """Poll a single EthernetIP device (runs in thread pool worker)"""
        try:
            with self.app.app_context():
                from models import EthernetIPConfig, EthernetIPTag, MQTTConfig
                
                config = self.db.session.query(EthernetIPConfig).get(config_id)
                if not config or not config.enabled:
                    return
                
                # Check if device is connected
                if not self.eip_service:
                    return
                
                status = self.eip_service.get_connection_status(config.id)
                if not status.get('connected', False):
                    return
                
                # Check device-level polling interval
                polling_interval_ms = config.polling_interval or 1000
                
                with self._lock:
                    if config.id in self._last_poll_time:
                        elapsed_ms = (datetime.utcnow() - self._last_poll_time[config.id]).total_seconds() * 1000
                        if elapsed_ms < polling_interval_ms:
                            return
                    self._last_poll_time[config.id] = datetime.utcnow()
                
                # Get all enabled tags
                tags = self.db.session.query(EthernetIPTag).filter_by(
                    config_id=config.id,
                    enabled=True
                ).all()
                
                # Collect tag values
                tag_data = {}
                for tag in tags:
                    try:
                        success, value = self.eip_service.read_tag(tag)
                        if success:
                            tag.last_value = str(value)
                            tag.last_read = datetime.utcnow()
                            self.db.session.commit()
                            tag_data[tag.tag_name] = value
                            
                            if self.data_logging_service:
                                self.data_logging_service.log_value(
                                    source_type='ethernetip',
                                    source_id=tag.id,
                                    source_name=f"{config.name}/{tag.tag_name}",
                                    value=value
                                )
                    except Exception as e:
                        logger.error(f"Error reading tag {tag.tag_name}: {str(e)}")
                
                # Publish collected data
                if tag_data:
                    device_log_key = f"eip_poll_{config.id}"
                    should_log = self._should_log(device_log_key)
                    if should_log:
                        logger.info(f"✓ Polled {config.name}: {len(tag_data)} tags")
                    self._publish_device_data(
                        MQTTConfig,
                        device_name=config.name,
                        device_config=config,
                        tag_data=tag_data,
                        log_publish=should_log
                    )
        
        except Exception as e:
            logger.error(f"Error polling EthernetIP device {config_id}: {str(e)}")
    
    def _poll_single_snmp_device(self, config_id):
        """Poll a single SNMP device (runs in thread pool worker)"""
        try:
            with self.app.app_context():
                from models import SNMPConfig, SNMPObject, MQTTConfig
                
                config = self.db.session.query(SNMPConfig).get(config_id)
                if not config or not config.enabled:
                    return
                
                # Check if device is connected
                if not self.snmp_service:
                    return
                
                status = self.snmp_service.get_connection_status(config.id)
                if not status.get('connected', False):
                    return
                
                # Check device-level polling interval
                device_id = f"snmp_{config.id}"
                polling_interval_ms = config.polling_interval or 1000
                
                with self._lock:
                    if device_id in self._last_poll_time:
                        elapsed_ms = (datetime.utcnow() - self._last_poll_time[device_id]).total_seconds() * 1000
                        if elapsed_ms < polling_interval_ms:
                            return
                    self._last_poll_time[device_id] = datetime.utcnow()
                
                # Get all enabled objects
                objects = self.db.session.query(SNMPObject).filter_by(
                    config_id=config.id,
                    enabled=True
                ).all()
                
                # Collect object values
                object_data = {}
                for obj in objects:
                    try:
                        success, value = self.snmp_service.read_oid(obj)
                        if success:
                            obj.last_value = str(value)
                            obj.last_read = datetime.utcnow()
                            self.db.session.commit()
                            
                            key = obj.description or obj.oid.replace('.', '_')
                            object_data[key] = value
                            
                            if self.data_logging_service:
                                self.data_logging_service.log_value(
                                    source_type='snmp',
                                    source_id=obj.id,
                                    source_name=f"{config.name}/{obj.oid}",
                                    value=value
                                )
                    except Exception as e:
                        logger.error(f"Error reading SNMP OID {obj.oid}: {str(e)}")
                
                # Publish collected data
                if object_data:
                    device_log_key = f"snmp_poll_{config.id}"
                    should_log = self._should_log(device_log_key)
                    if should_log:
                        logger.info(f"✓ Polled {config.name}: {len(object_data)} objects")
                    self._publish_device_data(
                        MQTTConfig,
                        device_name=config.name,
                        device_config=config,
                        tag_data=object_data,
                        log_publish=should_log
                    )
        
        except Exception as e:
            logger.error(f"Error polling SNMP device {config_id}: {str(e)}")
    
    def _reconnect_offline_devices(self, EthernetIPConfig, SNMPConfig, MQTTConfig):
        """Try to reconnect offline devices every 10 seconds"""
        try:
            current_time = datetime.utcnow()
            
            # Check EthernetIP devices
            if self.eip_service:
                eip_configs = self.db.session.query(EthernetIPConfig).filter_by(enabled=True).all()
                for config in eip_configs:
                    device_key = f"eip_{config.id}"
                    status = self.eip_service.get_connection_status(config.id)
                    
                    # If device is not connected, try to reconnect
                    if not status.get('connected', False):
                        # Check if we should attempt reconnection
                        if device_key in self._last_reconnect_attempt:
                            elapsed = (current_time - self._last_reconnect_attempt[device_key]).total_seconds()
                            if elapsed < self._reconnect_interval:
                                continue  # Too soon to retry
                        
                        # Attempt reconnection
                        self._last_reconnect_attempt[device_key] = current_time
                        logger.info(f"Attempting to reconnect EthernetIP device: {config.name}")
                        success, message = self.eip_service.connect_device(config)
                        if success:
                            logger.info(f"✓ Reconnected to {config.name}")
                        else:
                            logger.debug(f"✗ Reconnection failed for {config.name}: {message}")
            
            # Check SNMP devices
            if self.snmp_service:
                snmp_configs = self.db.session.query(SNMPConfig).filter_by(enabled=True).all()
                for config in snmp_configs:
                    device_key = f"snmp_{config.id}"
                    status = self.snmp_service.get_connection_status(config.id)
                    
                    # If device is not connected, try to reconnect
                    if not status.get('connected', False):
                        # Check if we should attempt reconnection
                        if device_key in self._last_reconnect_attempt:
                            elapsed = (current_time - self._last_reconnect_attempt[device_key]).total_seconds()
                            if elapsed < self._reconnect_interval:
                                continue  # Too soon to retry
                        
                        # Attempt reconnection
                        self._last_reconnect_attempt[device_key] = current_time
                        logger.info(f"Attempting to reconnect SNMP device: {config.name}")
                        success, message = self.snmp_service.connect_device(config)
                        if success:
                            logger.info(f"✓ Reconnected to {config.name}")
                        else:
                            logger.debug(f"✗ Reconnection failed for {config.name}: {message}")
            
            # Check MQTT brokers
            if self.mqtt_service:
                from models import MQTTConfig
                mqtt_configs = self.db.session.query(MQTTConfig).filter_by(enabled=True).all()
                for config in mqtt_configs:
                    device_key = f"mqtt_{config.id}"
                    status = self.mqtt_service.get_connection_status(config.id)
                    
                    # If broker is not connected, try to reconnect
                    if not status.get('connected', False):
                        # Check if we should attempt reconnection
                        if device_key in self._last_reconnect_attempt:
                            elapsed = (current_time - self._last_reconnect_attempt[device_key]).total_seconds()
                            if elapsed < self._reconnect_interval:
                                continue  # Too soon to retry
                        
                        # Attempt reconnection
                        self._last_reconnect_attempt[device_key] = current_time
                        logger.info(f"Attempting to reconnect MQTT broker: {config.name}")
                        success, message = self.mqtt_service.connect_broker(config)
                        if self.mqtt_service and config.subscribe_topic:
                            self.mqtt_service.restart_subscriber(config, self.app)
                        if success:
                            logger.info(f"✓ Reconnected to MQTT broker {config.name}")
                        else:
                            logger.debug(f"✗ Reconnection failed for {config.name}: {message}")
        
        except Exception as e:
            logger.error(f"Error in reconnect_offline_devices: {str(e)}")
    
    def _publish_device_data(self, MQTTConfig, device_name, device_config, tag_data, log_publish=True):
        """Publish all tags from a device in a single payload to MQTT brokers"""
        try:
            # Get all enabled MQTT brokers
            mqtt_configs = self.db.session.query(MQTTConfig).filter_by(enabled=True).all()
            
            if not mqtt_configs:
                return
            
            for mqtt_config in mqtt_configs:
                try:
                    if not self.mqtt_service:
                        logger.warning("MQTT service not available")
                        continue
                    
                    # Check if broker is connected
                    broker_status = self.mqtt_service.get_connection_status(mqtt_config.id)
                    if not broker_status.get('connected', False):
                        logger.debug(f"MQTT broker {mqtt_config.broker} not connected")
                        continue
                    
                    # Check if publish_topic is configured
                    if not mqtt_config.publish_topic:
                        logger.debug(f"No publish topic configured for MQTT broker {mqtt_config.name}")
                        continue
                    
                    # Use HWID if available, otherwise fall back to device ID
                    device_identifier = device_config.hwid if device_config.hwid else device_config.id
                    
                    # Use the configured publish topic directly (with device identifier appended)
                    topic = f"{mqtt_config.publish_topic}/{device_identifier}"
                    
                    # Get format preference
                    publish_format = mqtt_config.publish_format or 'json'
                    
                    # Format payload based on configuration
                    if publish_format.lower() == 'string':
                        # CSV format: HWID,Tag1_value,Tag2_value,...,Timestamp
                        timestamp = datetime.utcnow().isoformat()
                        tag_values = ','.join(str(v) for v in tag_data.values())
                        payload_str = f"{device_identifier},{tag_values},{timestamp}"
                    else:
                        # JSON format (default): {"HWID": hwid/id, "Tag1": value, "Tag2": value, ..., "Timestamp": "..."}
                        import json
                        payload = {
                            'HWID': device_identifier,
                            **tag_data,
                            'Timestamp': datetime.utcnow().isoformat()
                        }
                        payload_str = json.dumps(payload)
                    
                    # Publish to broker using the low-level publish method
                    success, message = self.mqtt_service.publish(mqtt_config, topic, payload_str)
                    
                    if success:
                        if log_publish:
                            logger.info(f"✓ Published {device_name} → {mqtt_config.name} ({topic})")
                    else:
                        logger.warning(f"✗ Failed to publish {device_name} to {mqtt_config.name}: {message}")
                
                except Exception as e:
                    logger.error(f"Error publishing to MQTT broker {mqtt_config.name}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error in publish_device_data: {str(e)}")
    
    def _publish_to_mqtt(self, MQTTConfig, topic, payload):
        """Publish data to all connected MQTT brokers"""
        try:
            if not self.mqtt_service:
                logger.warning("MQTT service not available")
                return
            
            # Get all enabled MQTT brokers
            mqtt_configs = self.db.session.query(MQTTConfig).filter_by(enabled=True).all()
            logger.info(f"Publishing to {len(mqtt_configs)} MQTT brokers")
            
            for mqtt_config in mqtt_configs:
                # Check if broker is connected
                status = self.mqtt_service.get_connection_status(mqtt_config.id)
                if not status.get('connected', False):
                    logger.warning(f"MQTT broker {mqtt_config.name} not connected, skipping")
                    continue
                
                # Build full topic (service adds prefix)
                
                # Publish message
                success, message = self.mqtt_service.publish(
                    mqtt_config,
                    topic,
                    payload
                )
                
                if success:
                    logger.info(f"✓ Published to {mqtt_config.name}: {topic}")
                else:
                    logger.warning(f"✗ Failed to publish to {mqtt_config.name}: {message}")
        
        except Exception as e:
            logger.error(f"Error publishing to MQTT: {str(e)}")
    
    def _should_log(self, log_key):
        """Determine if we should log based on interval (throttling)"""
        current_time = datetime.utcnow()
        
        with self._lock:
            if log_key not in self._last_log_time:
                self._last_log_time[log_key] = current_time
                return True
            
            elapsed = (current_time - self._last_log_time[log_key]).total_seconds()
            if elapsed >= self._log_interval:
                self._last_log_time[log_key] = current_time
                return True
        
        return False
    
    def get_status(self):
        """Get polling service status"""
        return {
            'running': self._running,
            'eip_thread_alive': self._eip_thread.is_alive() if self._eip_thread else False,
            'snmp_thread_alive': self._snmp_thread.is_alive() if self._snmp_thread else False,
            'reconnect_thread_alive': self._reconnect_thread.is_alive() if self._reconnect_thread else False
        }
