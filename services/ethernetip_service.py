import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)

# Enable mock mode via environment variable
USE_MOCK_PLC = True

class EthernetIPService:
    def __init__(self):
        self._connection_status = {}  # Dict to store status per device ID
        self._active_connections = {}  # Dict to store active connections per device ID
        
        if USE_MOCK_PLC:
            logger.info("=" * 60)
            logger.info("EthernetIP Service initialized with MOCK PLC")
            logger.info("=" * 60)
            print("EthernetIP Service initialized with MOCK PLC")  # Force console output
        else:
            logger.info("=" * 60)
            logger.info("EthernetIP Service initialized with REAL PLC")
            logger.info("=" * 60)
            print("EthernetIP Service initialized with REAL PLC")  # Force console output
    
    def _get_plc_client(self):
        """Get PLC client (real or mock based on environment)"""
        if USE_MOCK_PLC:
            from ethernetip_simulator import MockEthernetIPClient
            return MockEthernetIPClient()
        else:
            from pylogix import PLC
            return PLC()
    
    def get_connection_status(self, device_id=None):
        """Get connection status for specific device or all devices"""
        if device_id:
            status = self._connection_status.get(device_id, {
                'connected': False,
                'last_check': None,
                'message': 'Not connected'
            })
            return {
                'success': True,
                'connected': status.get('connected', False),
                'message': status.get('message', 'Unknown'),
                'last_check': status.get('last_check').isoformat() if status.get('last_check') else None
            }
        return self._connection_status
    
    def connect_device(self, config):
        """Establish connection to a device and keep it active"""
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = config.ip_address
                comm.ProcessorSlot = config.slot
                comm.SocketTimeout = config.timeout
                
                ret = comm.GetPLCTime()
                
                if ret.Status == 'Success':
                    self._connection_status[config.id] = {
                        'connected': True,
                        'last_check': datetime.utcnow(),
                        'message': f'Connected to {config.ip_address}'
                    }
                    logger.info(f"Connected to EthernetIP device {config.name} at {config.ip_address}")
                    return True, f"Connected successfully"
                else:
                    self._connection_status[config.id] = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': f'Connection failed: {ret.Status}'
                    }
                    logger.debug(f"EthernetIP connection failed for {config.name}: {ret.Status}")
                    return False, ret.Status
        except Exception as e:
            logger.debug(f"Failed to connect to EthernetIP device {config.name}: {str(e)}")
            self._connection_status[config.id] = {
                'connected': False,
                'last_check': datetime.utcnow(),
                'message': str(e)
            }
            return False, str(e)
    
    def discover_tags(self, config):
        """Discover all available tags from a PLC"""
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = config.ip_address
                comm.ProcessorSlot = config.slot
                comm.SocketTimeout = config.timeout
                
                ret = comm.GetTagList()
                
                if ret.Status == 'Success':
                    tags = []
                    if isinstance(ret.Value, list):
                        for tag in ret.Value:
                            if isinstance(tag, dict):
                                tags.append({
                                    'name': tag.get('TagName', 'Unknown'),
                                    'data_type': tag.get('DataType', 'Unknown')
                                })
                            else:
                                # Handle if tag is an object with attributes
                                tags.append({
                                    'name': getattr(tag, 'TagName', 'Unknown'),
                                    'data_type': getattr(tag, 'DataType', 'Unknown')
                                })
                    logger.info(f"Discovered {len(tags)} tags from {config.ip_address}")
                    return True, tags
                else:
                    return False, f"Discovery failed: {ret.Status}"
        except Exception as e:
            logger.error(f"Tag discovery failed: {str(e)}")
            return False, str(e)
    
    def test_connection(self, config):
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = config.ip_address
                comm.ProcessorSlot = config.slot
                comm.SocketTimeout = config.timeout
                
                ret = comm.GetPLCTime()
                
                if ret.Status == 'Success':
                    self._connection_status = {
                        'connected': True,
                        'last_check': datetime.utcnow(),
                        'message': f'Connected to {config.ip_address}'
                    }
                    return True, f"PLC Time: {ret.Value}"
                else:
                    self._connection_status = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': f'Connection failed: {ret.Status}'
                    }
                    return False, ret.Status
                    
        except Exception as e:
            logger.error(f"EthernetIP connection test failed: {str(e)}")
            self._connection_status = {
                'connected': False,
                'last_check': datetime.utcnow(),
                'message': str(e)
            }
            return False, str(e)
    
    def read_tag(self, tag):
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = tag.config.ip_address
                comm.ProcessorSlot = tag.config.slot
                comm.SocketTimeout = tag.config.timeout
                
                ret = comm.Read(tag.tag_name)
                
                if ret.Status == 'Success':
                    return True, ret.Value
                else:
                    return False, ret.Status
                    
        except Exception as e:
            logger.error(f"Tag read failed: {str(e)}")
            return False, str(e)
    
    def write_tag(self, tag, value):
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = tag.config.ip_address
                comm.ProcessorSlot = tag.config.slot
                comm.SocketTimeout = tag.config.timeout
                
                ret = comm.Write(tag.tag_name, value)
                
                if ret.Status == 'Success':
                    return True, "Write successful"
                else:
                    return False, ret.Status
                    
        except Exception as e:
            logger.error(f"Tag write failed: {str(e)}")
            return False, str(e)
    
    def get_tag_list(self, config):
        try:
            with self._get_plc_client() as comm:
                comm.IPAddress = config.ip_address
                comm.ProcessorSlot = config.slot
                comm.SocketTimeout = config.timeout
                
                ret = comm.GetTagList()
                
                if ret.Status == 'Success':
                    return True, ret.Value
                else:
                    return False, ret.Status
                    
        except Exception as e:
            logger.error(f"Get tag list failed: {str(e)}")
            return False, str(e)
