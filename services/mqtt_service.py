import logging
from datetime import datetime
import json
import threading

logger = logging.getLogger(__name__)

class MQTTService:
    def __init__(self):
        self._connection_status = {}  # Dict to store status per config ID
        self._clients = {}  # Active MQTT clients: {config_id: client}
        self._lock = threading.Lock()
    
    def cleanup(self):
        """Cleanup all persistent MQTT connections"""
        with self._lock:
            # Cleanup publishers
            for config_id, client in self._clients.items():
                try:
                    client.loop_stop()
                    client.disconnect()
                    logger.info(f"Closed MQTT client for config {config_id}")
                except Exception as e:
                    logger.error(f"Error closing MQTT client {config_id}: {str(e)}")
            self._clients.clear()
            
            # Cleanup subscribers
            if hasattr(self, '_subscribers'):
                for config_id, client in self._subscribers.items():
                    try:
                        client.loop_stop()
                        client.disconnect()
                        logger.info(f"Closed MQTT subscriber for config {config_id}")
                    except Exception as e:
                        logger.error(f"Error closing MQTT subscriber {config_id}: {str(e)}")
                self._subscribers.clear()
    
    def get_connection_status(self, config_id=None):
        """Get connection status for specific config or all configs"""
        with self._lock:
            if config_id:
                return self._connection_status.get(config_id, {
                    'connected': False,
                    'last_check': None,
                    'message': 'Not connected'
                })
            return self._connection_status
    
    def connect_broker(self, config):
        """Establish connection to MQTT broker"""
        try:
            import paho.mqtt.client as mqtt
            
            result = {'connected': False, 'message': ''}
            
            def on_connect(client, userdata, flags, rc, properties=None):
                if rc == 0:
                    result['connected'] = True
                    result['message'] = 'Connected successfully'
                else:
                    result['message'] = f'Connection failed with code {rc}'
            
            # Create client with version compatibility
            try:
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            except AttributeError:
                client = mqtt.Client()
            
            client.on_connect = on_connect
            
            if config.username and config.password:
                client.username_pw_set(config.username, config.password)
            
            if config.use_tls:
                client.tls_set()
            
            try:
                client.connect(config.broker, config.port, 60)
                client.loop_start()
                
                import time
                timeout = 5
                start = time.time()
                while not result['connected'] and time.time() - start < timeout:
                    time.sleep(0.1)
                
                client.loop_stop()
                client.disconnect()
                
                with self._lock:
                    self._connection_status[config.id] = {
                        'connected': result['connected'],
                        'last_check': datetime.utcnow(),
                        'message': result['message']
                    }
                
                if result['connected']:
                    logger.info(f"Connected to MQTT broker {config.name} at {config.broker}")
                    return True, result['message']
                else:
                    return False, result['message']
                    
            except Exception as e:
                logger.error(f"MQTT connection error: {str(e)}")
                with self._lock:
                    self._connection_status[config.id] = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': str(e)
                    }
                return False, str(e)
                
        except Exception as e:
            logger.error(f"MQTT connection failed: {str(e)}")
            with self._lock:
                self._connection_status[config.id] = {
                    'connected': False,
                    'last_check': datetime.utcnow(),
                    'message': str(e)
                }
            return False, str(e)
    
    def test_connection(self, config):
        try:
            import paho.mqtt.client as mqtt
            
            result = {'connected': False, 'message': ''}
            
            def on_connect(client, userdata, flags, rc, properties=None):
                if rc == 0:
                    result['connected'] = True
                    result['message'] = 'Connection successful'
                else:
                    result['message'] = f'Connection failed with code {rc}'
            
            # Create client with version compatibility
            try:
                # Try version 2.0+ API
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            except AttributeError:
                # Fall back to older API
                client = mqtt.Client()
            
            client.on_connect = on_connect
            
            if config.username and config.password:
                client.username_pw_set(config.username, config.password)
            
            if config.use_tls:
                client.tls_set()
            
            try:
                client.connect(config.broker, config.port, 60)
                client.loop_start()
                
                import time
                timeout = 5
                start = time.time()
                while not result['connected'] and time.time() - start < timeout:
                    time.sleep(0.1)
                
                client.loop_stop()
                client.disconnect()
                
                if result['connected']:
                    self._connection_status = {
                        'connected': True,
                        'last_check': datetime.utcnow(),
                        'message': f'Connected to {config.broker}'
                    }
                    return True, result['message']
                else:
                    if not result['message']:
                        result['message'] = 'Connection timeout'
                    self._connection_status = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': result['message']
                    }
                    return False, result['message']
                    
            except Exception as e:
                self._connection_status = {
                    'connected': False,
                    'last_check': datetime.utcnow(),
                    'message': str(e)
                }
                return False, str(e)
                
        except Exception as e:
            logger.error(f"MQTT connection test failed: {str(e)}")
            self._connection_status = {
                'connected': False,
                'last_check': datetime.utcnow(),
                'message': str(e)
            }
            return False, str(e)
    
    def publish(self, config, topic, value):
        """Publish to MQTT using persistent connection"""
        try:
            import paho.mqtt.client as mqtt
            
            with self._lock:
                # Get or create persistent client for this broker
                if config.id not in self._clients:
                    # Create new persistent client
                    try:
                        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                    except AttributeError:
                        client = mqtt.Client()
                    
                    if config.username and config.password:
                        client.username_pw_set(config.username, config.password)
                    
                    if config.use_tls:
                        client.tls_set()
                    
                    try:
                        client.connect(config.broker, config.port, 60)
                        client.loop_start()
                        self._clients[config.id] = client
                        logger.debug(f"Created persistent MQTT client for {config.name}")
                    except Exception as e:
                        logger.error(f"Failed to create MQTT client for {config.name}: {str(e)}")
                        return False, str(e)
                
                client = self._clients[config.id]
            
            # Topic is already formatted with prefix from polling service
            full_topic = topic
            
            if isinstance(value, (dict, list)):
                payload = json.dumps(value)
            else:
                payload = str(value)
            
            # Publish using persistent connection
            result = client.publish(full_topic, payload)
            
            # Don't wait for publish to avoid blocking
            # result.wait_for_publish()
            
            return True, f"Published to {full_topic}"
            
        except Exception as e:
            logger.error(f"MQTT publish failed: {str(e)}")
            # Remove failed client so it will be recreated
            with self._lock:
                if config.id in self._clients:
                    try:
                        self._clients[config.id].loop_stop()
                        self._clients[config.id].disconnect()
                    except:
                        pass
                    del self._clients[config.id]
            return False, str(e)
    
    def start_subscriber(self, config, flask_app=None):
        """Start MQTT subscriber for two-way communication"""
        if not config.subscribe_topic:
            logger.debug(f"No subscribe topic configured for {config.name}")
            return True, "No subscription needed"
            
        try:
            import paho.mqtt.client as mqtt
            
            # Store Flask app reference for use in callbacks
            app_ref = flask_app
            if not app_ref:
                try:
                    from flask import current_app
                    app_ref = current_app._get_current_object()
                except RuntimeError:
                    logger.error("Flask app context not available and no app passed")
                    return False, "Flask app context not available"
            
            def on_connect(client, userdata, flags, rc, properties=None):
                if rc == 0:
                    client.subscribe(config.subscribe_topic+"/#")  # Subscribe to all subtopics
                    logger.info(f"Subscribed to topic '{config.subscribe_topic}' for config {config.name}")
                    with self._lock:
                        self._connection_status[config.id] = {
                            'connected': True,
                            'last_check': datetime.utcnow(),
                            'message': f'Subscribed to {config.subscribe_topic}'
                        }
                else:
                    logger.error(f"MQTT subscriber connection failed with code {rc}")
            
            def on_message(client, userdata, msg):
                """Handle incoming MQTT messages for SNMP write operations"""
                try:
                    payload = msg.payload.decode('utf-8')
                    logger.debug(f"Received MQTT message on {msg.topic}: {payload}")
                    
                    # Parse JSON message
                    try:
                        data = json.loads(payload)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in MQTT message: {str(e)}")
                        return
                    
                    # Validate required fields
                    required_fields = ['device_id', 'Parameter_Name', 'value']
                    for field in required_fields:
                        if field not in data:
                            logger.error(f"Missing required field '{field}' in MQTT message")
                            return
                    
                    device_id = data['device_id']
                    parameter_name = data['Parameter_Name']
                    value = data['value']
                    message_id = data.get('message_id', 'unknown')
                    
                    # Extract HWID from topic if using wildcard subscription
                    # Topic format: Test_SNMP_Get/HWID
                    topic_parts = msg.topic.split('/')
                    topic_hwid = None
                    if len(topic_parts) >= 2:
                        topic_hwid = topic_parts[-1]  # Last part should be HWID
                        logger.info(f"HWID from topic: {topic_hwid}")
                    
                    # Use HWID from topic if available, otherwise use device_id from message
                    hwid_to_search = topic_hwid if topic_hwid else device_id
                    
                    logger.info(f"Processing SNMP write command: device_id={device_id}, topic_hwid={topic_hwid}, parameter={parameter_name}, value={value}, msg_id={message_id}")
                    
                    # Get Flask app instance and create application context
                    with app_ref.app_context():
                        # Find SNMP config by HWID (from topic or device_id)
                        from models import SNMPConfig
                        from database import db
                        
                        snmp_config = db.session.query(SNMPConfig).filter_by(hwid=hwid_to_search).first()
                        if not snmp_config:
                            logger.warning(f"No SNMP configuration found for HWID: {hwid_to_search}")
                            return
                        
                        # Get SNMP service and perform write operation
                        snmp_service = app_ref.config.get('snmp_service')
                        if not snmp_service:
                            logger.error("SNMP service not available")
                            return
                        
                        # Write to SNMP device
                        success, message = snmp_service.write_by_name(snmp_config, parameter_name, value)
                        
                        if success:
                            logger.info(f"Successfully wrote '{value}' to '{parameter_name}' on device {hwid_to_search}")
                            # Optionally publish confirmation message
                            if config.publish_topic:
                                confirmation = {
                                    "device_id": device_id,
                                    "hwid": hwid_to_search,
                                    "topic": msg.topic,
                                    "Parameter_Name": parameter_name,
                                    "value": value,
                                    "message_id": message_id,
                                    "status": "success",
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                                client.publish(f"{config.publish_topic}/confirmation", json.dumps(confirmation))
                        else:
                            logger.error(f"Failed to write '{value}' to '{parameter_name}' on device {hwid_to_search}: {message}")
                            # Publish error message
                            if config.publish_topic:
                                error_msg = {
                                    "device_id": device_id,
                                    "hwid": hwid_to_search,
                                    "topic": msg.topic,
                                    "Parameter_Name": parameter_name,
                                    "value": value,
                                    "message_id": message_id,
                                    "status": "error",
                                    "error": message,
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                                client.publish(f"{config.publish_topic}/error", json.dumps(error_msg))
                    
                except Exception as e:
                    logger.error(f"Error processing MQTT message: {str(e)}", exc_info=True)
            
            def on_disconnect(client, userdata, rc, properties=None):
                if rc != 0:
                    logger.warning(f"MQTT subscriber unexpectedly disconnected with code {rc}")
                    with self._lock:
                        if config.id in self._connection_status:
                            self._connection_status[config.id]['connected'] = False
                            self._connection_status[config.id]['message'] = f'Disconnected (code {rc})'
                
            # Create subscriber client
            try:
                subscriber_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"snmp_bridge_sub_{config.id}")
            except AttributeError:
                subscriber_client = mqtt.Client(client_id=f"snmp_bridge_sub_{config.id}")
                
            subscriber_client.on_connect = on_connect
            subscriber_client.on_message = on_message
            subscriber_client.on_disconnect = on_disconnect
            
            if config.username and config.password:
                subscriber_client.username_pw_set(config.username, config.password)
            
            if config.use_tls:
                subscriber_client.tls_set()
            
            # Start subscriber in background
            subscriber_client.connect(config.broker, config.port, 60)
            subscriber_client.loop_start()
            
            # Store subscriber client separately
            with self._lock:
                if not hasattr(self, '_subscribers'):
                    self._subscribers = {}
                self._subscribers[config.id] = subscriber_client
            
            logger.info(f"Started MQTT subscriber for {config.name} on topic '{config.subscribe_topic}'")
            return True, "Subscriber started successfully"
            
        except Exception as e:
            logger.error(f"Failed to start MQTT subscriber: {str(e)}")
            return False, str(e)
    
    def stop_subscriber(self, config_id):
        """Stop MQTT subscriber for a configuration"""
        try:
            with self._lock:
                if hasattr(self, '_subscribers') and config_id in self._subscribers:
                    client = self._subscribers[config_id]
                    client.loop_stop()
                    client.disconnect()
                    del self._subscribers[config_id]
                    logger.info(f"Stopped MQTT subscriber for config {config_id}")
                    return True, "Subscriber stopped"
                else:
                    return True, "No active subscriber found"
        except Exception as e:
            logger.error(f"Error stopping subscriber: {str(e)}")
            return False, str(e)
    
    def restart_subscriber(self, config, flask_app=None):
        """Restart subscriber for updated configuration"""
        self.stop_subscriber(config.id)
        return self.start_subscriber(config, flask_app)
