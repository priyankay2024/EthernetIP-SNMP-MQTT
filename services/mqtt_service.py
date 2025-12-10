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
            for config_id, client in self._clients.items():
                try:
                    client.loop_stop()
                    client.disconnect()
                    logger.info(f"Closed MQTT client for config {config_id}")
                except Exception as e:
                    logger.error(f"Error closing MQTT client {config_id}: {str(e)}")
            self._clients.clear()
    
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
