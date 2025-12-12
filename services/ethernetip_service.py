import logging
from datetime import datetime
import os
import json

logger = logging.getLogger(__name__)

# =====================================================================
# IMPLEMENTATION SELECTOR - Choose ONE:
# =====================================================================
# Options:
#   - "PYLOGIX"  : Use PyLogix library (works with real PLCs)
#   - "CPPPO"    : Use CPPo native client (for cpppo servers)
#   - "MOCK"     : Use Mock simulator (for testing without hardware)
# =====================================================================
USE_IMPLEMENTATION = "PYLOGIX" # os.getenv("ETHERNETIP_IMPLEMENTATION", "PYLOGIX").upper()
USE_MOCK_PLC = USE_IMPLEMENTATION == "MOCK"
USE_CPPPO_CLIENT = USE_IMPLEMENTATION == "CPPPO"


# =====================================================================
# CPPPO NATIVE CLIENT WRAPPER CLASS
# =====================================================================
class CPPoPLCClient:
    """
    CPPo Native Client - Uses cpppo's built-in client function for EtherNet/IP communication.
    This allows communication with cpppo servers and other EtherNet/IP devices.
    """
    
    def __init__(self):
        self.IPAddress = None
        self.ProcessorSlot = 0
        self.SocketTimeout = 5.0
        self.host = None
        self.port = 2222  # Default EtherNet/IP port
        self._session = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
    
    def _get_host_port(self):
        """Extract host and port from IPAddress"""
        if not self.IPAddress:
            raise ValueError("IPAddress not set")
        
        if ':' in self.IPAddress:
            host, port = self.IPAddress.split(':')
            return host, int(port)
        return self.IPAddress, self.port
    
    def GetPLCTime(self):
        """Test connection by registering a session with the EtherNet/IP device"""
        try:
            import socket
            import struct
            
            host, port = self._get_host_port()
            
            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.SocketTimeout)
            
            try:
                sock.connect((host, port))
                
                # Send EtherNet/IP RegisterSession request (28 bytes)
                # Structure:
                #   Command (u16): 0x65 (RegisterSession)
                #   Length (u16): 0 (no encapsulation data)
                #   SessionHandle (u32): 0 (not yet registered)
                #   Status (u32): 0
                #   SenderContext (u64): 0
                #   Options (u32): 0
                register_request = struct.pack('<HHIIQI',
                    0x65,           # Command: RegisterSession
                    0,              # Length: 0 (no additional data)
                    0,              # Session Handle: 0 (not yet registered)
                    0,              # Status: 0
                    0,              # Sender Context: 0
                    0               # Options: 0
                )
                
                sock.send(register_request)
                
                # Receive response (should be 28 bytes for RegisterSession response)
                response = sock.recv(1024)
                
                if len(response) >= 28:
                    # Parse response: Command, Length, SessionHandle, Status, SenderContext, Options
                    cmd, length, session_handle, status = struct.unpack('<HHII', response[:12])
                    if status == 0:  # Success status (0 = no error)
                        class Response:
                            Status = 'Success'
                            Value = datetime.utcnow()
                        return Response()
                    else:
                        class Response:
                            Status = f'EtherNet/IP error status: {status}'
                            Value = None
                        return Response()
                else:
                    class Response:
                        Status = f'Incomplete response: received {len(response)} bytes, expected 28'
                        Value = None
                    return Response()
                
            finally:
                sock.close()
            
        except Exception as e:
            logger.debug(f"CPPo GetPLCTime error: {str(e)}")
            class Response:
                Status = str(e)
                Value = None
            return Response()
    
    def GetTagList(self):
        """
        Discover all tags on the device.
        CPPo doesn't have direct tag discovery like PyLogix, so we return a helpful message.
        """
        try:
            # CPPo is primarily a server simulator, not designed for tag discovery
            # Return empty list or known tags
            logger.warning("CPPo client: GetTagList not natively supported. Configure tags manually.")
            
            class Response:
                Status = 'GetTagList not supported in CPPo native mode. Please add tags manually.'
                Value = []
            return Response()
            
        except Exception as e:
            logger.debug(f"CPPo GetTagList error: {str(e)}")
            class Response:
                Status = str(e)
                Value = None
            return Response()
    
    def Read(self, tag_name):
        """Read a tag value from the cpppo server"""
        try:
            from cpppo.server.enip import client as enip_client
            
            host, port = self._get_host_port()
            
            # Use cpppo's client generator with proper CIP request
            # The client() function expects host, port, and yields parsed responses
            results = []
            
            try:
                for reply in enip_client.client(
                    host=host,
                    port=port,
                    timeout=self.SocketTimeout
                ):
                    if reply:
                        results.append(reply)
                        # Break after first response
                        break
                
                if results and 'Value' in results[0]:
                    class Response:
                        Status = 'Success'
                        Value = results[0].get('Value')
                    return Response()
                    
            except StopIteration:
                pass
            
            class Response:
                Status = f'Failed to read tag {tag_name}'
                Value = None
            return Response()
            
        except Exception as e:
            logger.debug(f"CPPo Read error: {str(e)}")
            class Response:
                Status = str(e)
                Value = None
            return Response()
    
    def Write(self, tag_name, value):
        """Write a tag value to the cpppo server"""
        try:
            from cpppo.server.enip import client as enip_client
            
            host, port = self._get_host_port()
            
            # Use cpppo's client generator
            results = []
            
            try:
                for reply in enip_client.client(
                    host=host,
                    port=port,
                    timeout=self.SocketTimeout
                ):
                    if reply:
                        results.append(reply)
                        break
                
                if results and results[0].get('status') == 0:
                    class Response:
                        Status = 'Success'
                        Value = value
                    return Response()
                    
            except StopIteration:
                pass
            
            class Response:
                Status = f'Failed to write tag {tag_name}'
                Value = None
            return Response()
            
        except Exception as e:
            logger.debug(f"CPPo Write error: {str(e)}")
            class Response:
                Status = str(e)
                Value = None
            return Response()

class EthernetIPService:
    def __init__(self):
        self._connection_status = {}  # Dict to store status per device ID
        self._active_connections = {}  # Dict to store active connections per device ID
        
        logger.info("=" * 70)
        if USE_MOCK_PLC:
            logger.info("EthernetIP Service initialized with MOCK PLC SIMULATOR")
            print("EthernetIP Service initialized with MOCK PLC SIMULATOR")
        elif USE_CPPPO_CLIENT:
            logger.info("EthernetIP Service initialized with CPPPO NATIVE CLIENT")
            print("EthernetIP Service initialized with CPPPO NATIVE CLIENT")
        else:
            logger.info("EthernetIP Service initialized with PYLOGIX CLIENT")
            print("EthernetIP Service initialized with PYLOGIX CLIENT")
        logger.info("=" * 70)
    
    def _get_plc_client(self):
        """Get PLC client based on selected implementation"""
        if USE_MOCK_PLC:
            from ethernetip_simulator import MockEthernetIPClient
            return MockEthernetIPClient()
        elif USE_CPPPO_CLIENT:
            return CPPoPLCClient()
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
                    # Handle specific PLC response errors
                    error_msg = ret.Status
                    
                    if "Attribute not gettable" in error_msg or "not gettable" in error_msg.lower():
                        logger.warning(f"GetTagList service not supported on this PLC (Slot {config.slot})")
                        return False, "This PLC or slot does not support automatic tag discovery. You can manually add tags by entering their names in the configuration, or try a different slot number."
                    elif "Connection failure" in error_msg or "not connected" in error_msg.lower():
                        return False, f"Unable to connect to PLC at {config.ip_address}:{config.slot}. Please verify the IP address and slot are correct."
                    else:
                        return False, f"Discovery failed: {error_msg}"
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
                    return True, f"Connection successful"
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
    
    def detect_devices(self, ip_range, port=2222, timeout=5.0):
        """Detect EthernetIP devices in a given IP range"""
        import ipaddress
        import threading
        
        try:
            network = ipaddress.ip_network(ip_range, strict=False)
            devices = []
            lock = threading.Lock()
            
            def check_device(ip):
                try:
                    with self._get_plc_client() as comm:
                        comm.IPAddress = str(ip)
                        comm.ProcessorSlot = 0
                        comm.SocketTimeout = timeout
                        
                        ret = comm.GetPLCTime()
                        
                        if ret.Status == 'Success':
                            with lock:
                                devices.append({
                                    'ip_address': str(ip),
                                    'slot': 0,
                                    'timeout': timeout,
                                    'polling_interval': 1000
                                })
                            logger.info(f"Found EthernetIP device at {ip}")
                except:
                    pass
            
            # Create threads to check each IP
            threads = []
            batch_size = 10  # Check 10 IPs in parallel
            
            for ip in list(network.hosts()):
                thread = threading.Thread(target=check_device, args=(ip,), daemon=True)
                thread.start()
                threads.append(thread)
                
                # Limit concurrent threads
                if len(threads) >= batch_size:
                    for t in threads:
                        t.join(timeout=timeout + 1)
                    threads = [t for t in threads if t.is_alive()]
            
            # Wait for remaining threads
            for thread in threads:
                thread.join(timeout=timeout + 1)
            
            logger.info(f"Device detection completed. Found {len(devices)} devices")
            return True, devices
            
        except ValueError as e:
            logger.error(f"Invalid IP range: {str(e)}")
            return False, f"Invalid IP range: {str(e)}"
        except Exception as e:
            logger.error(f"Device detection failed: {str(e)}")
            return False, str(e)
