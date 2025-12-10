"""
Mock EthernetIP Client for Testing
Simulates a PLC with various data types and tags
"""
import logging
import random
import time
from datetime import datetime
from threading import Thread, Lock

logger = logging.getLogger(__name__)


class MockTag:
    """Represents a simulated PLC tag"""
    def __init__(self, name, data_type, initial_value=0):
        self.name = name
        self.data_type = data_type
        self.value = initial_value
        self.last_updated = datetime.utcnow()
    
    def update_value(self, value):
        """Update tag value"""
        self.value = value
        self.last_updated = datetime.utcnow()
    
    def get_value(self):
        """Get current tag value"""
        return self.value


class MockPLC:
    """Mock PLC server that simulates EthernetIP communication"""
    
    def __init__(self, ip_address="127.0.0.1", slot=0):
        self.ip_address = ip_address
        self.slot = slot
        self.tags = {}
        self.connected = True
        self.lock = Lock()
        self.simulation_thread = None
        self.running = False
        
        # Initialize default tags
        self._initialize_default_tags()
        logger.info(f"Mock PLC initialized at {ip_address}:{slot}")
    
    def _initialize_default_tags(self):
        """Create default tags for testing"""
        # Digital/Boolean tags
        self.tags['System_Running'] = MockTag('System_Running', 'BOOL', True)
        self.tags['Emergency_Stop'] = MockTag('Emergency_Stop', 'BOOL', False)
        self.tags['Motor_1_Status'] = MockTag('Motor_1_Status', 'BOOL', True)
        self.tags['Motor_2_Status'] = MockTag('Motor_2_Status', 'BOOL', False)
        
        # Analog/Integer tags
        self.tags['Temperature_1'] = MockTag('Temperature_1', 'REAL', 25.5)
        self.tags['Temperature_2'] = MockTag('Temperature_2', 'REAL', 30.2)
        self.tags['Pressure'] = MockTag('Pressure', 'REAL', 101.3)
        self.tags['Flow_Rate'] = MockTag('Flow_Rate', 'REAL', 150.0)
        self.tags['Speed_Setpoint'] = MockTag('Speed_Setpoint', 'DINT', 1500)
        self.tags['Counter_1'] = MockTag('Counter_1', 'DINT', 0)
        self.tags['Production_Count'] = MockTag('Production_Count', 'DINT', 1000)
        
        # Array tags
        self.tags['Sensor_Array[0]'] = MockTag('Sensor_Array[0]', 'REAL', 10.0)
        self.tags['Sensor_Array[1]'] = MockTag('Sensor_Array[1]', 'REAL', 20.0)
        self.tags['Sensor_Array[2]'] = MockTag('Sensor_Array[2]', 'REAL', 30.0)
        
        logger.info(f"Initialized {len(self.tags)} default tags")
    
    def start_simulation(self):
        """Start simulating tag value changes"""
        if not self.running:
            self.running = True
            self.simulation_thread = Thread(target=self._simulate_values, daemon=True)
            self.simulation_thread.start()
            logger.info("Tag simulation started")
    
    def stop_simulation(self):
        """Stop simulating tag value changes"""
        self.running = False
        if self.simulation_thread:
            self.simulation_thread.join(timeout=2)
        logger.info("Tag simulation stopped")
    
    def _simulate_values(self):
        """Background thread to simulate changing tag values"""
        while self.running:
            try:
                with self.lock:
                    # Simulate temperature fluctuations
                    if 'Temperature_1' in self.tags:
                        current = self.tags['Temperature_1'].value
                        self.tags['Temperature_1'].update_value(current + random.uniform(-0.5, 0.5))
                    
                    if 'Temperature_2' in self.tags:
                        current = self.tags['Temperature_2'].value
                        self.tags['Temperature_2'].update_value(current + random.uniform(-0.3, 0.3))
                    
                    # Simulate pressure changes
                    if 'Pressure' in self.tags:
                        current = self.tags['Pressure'].value
                        self.tags['Pressure'].update_value(current + random.uniform(-1.0, 1.0))
                    
                    # Increment counter
                    if 'Counter_1' in self.tags:
                        self.tags['Counter_1'].update_value(self.tags['Counter_1'].value + 1)
                    
                    # Simulate flow rate
                    if 'Flow_Rate' in self.tags:
                        base = 150.0
                        self.tags['Flow_Rate'].update_value(base + random.uniform(-10, 10))
                
                time.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Simulation error: {e}")
    
    def read_tag(self, tag_name):
        """Read a tag value"""
        with self.lock:
            if not self.connected:
                return MockResponse('Disconnected', None)
            
            if tag_name in self.tags:
                tag = self.tags[tag_name]
                logger.debug(f"Read tag {tag_name}: {tag.value}")
                return MockResponse('Success', tag.value)
            else:
                logger.warning(f"Tag not found: {tag_name}")
                return MockResponse('TagNotFound', None)
    
    def write_tag(self, tag_name, value):
        """Write a value to a tag"""
        with self.lock:
            if not self.connected:
                return MockResponse('Disconnected', None)
            
            if tag_name in self.tags:
                self.tags[tag_name].update_value(value)
                logger.debug(f"Write tag {tag_name}: {value}")
                return MockResponse('Success', value)
            else:
                # Allow creating new tags on write
                self.tags[tag_name] = MockTag(tag_name, 'DINT', value)
                logger.info(f"Created new tag {tag_name} with value: {value}")
                return MockResponse('Success', value)
    
    def get_tag_list(self):
        """Get list of all available tags"""
        with self.lock:
            if not self.connected:
                return MockResponse('Disconnected', [])
            
            tag_list = [{'TagName': name, 'DataType': tag.data_type} 
                       for name, tag in self.tags.items()]
            return MockResponse('Success', tag_list)
    
    def get_plc_time(self):
        """Get PLC time"""
        if not self.connected:
            return MockResponse('Disconnected', None)
        
        return MockResponse('Success', datetime.utcnow())
    
    def disconnect(self):
        """Simulate disconnection"""
        self.connected = False
        logger.info("Mock PLC disconnected")
    
    def connect(self):
        """Simulate connection"""
        self.connected = True
        logger.info("Mock PLC connected")


class MockResponse:
    """Mock response object matching pylogix response structure"""
    def __init__(self, status, value):
        self.Status = status
        self.Value = value


class MockEthernetIPClient:
    """
    Mock EthernetIP Client that mimics pylogix PLC interface
    Compatible with existing EthernetIPService code
    """
    
    # Class-level mock PLC instances (simulates multiple PLCs)
    _plc_instances = {}
    
    def __init__(self):
        self.IPAddress = None
        self.ProcessorSlot = 0
        self.SocketTimeout = 5.0
        self._plc = None
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        pass
    
    def _get_or_create_plc(self):
        """Get or create a mock PLC instance for the configured IP"""
        if not self.IPAddress:
            raise ValueError("IPAddress not set")
        
        key = f"{self.IPAddress}:{self.ProcessorSlot}"
        
        if key not in MockEthernetIPClient._plc_instances:
            plc = MockPLC(self.IPAddress, self.ProcessorSlot)
            plc.start_simulation()  # Auto-start simulation
            MockEthernetIPClient._plc_instances[key] = plc
            logger.info(f"Created new Mock PLC instance: {key}")
        
        return MockEthernetIPClient._plc_instances[key]
    
    def GetPLCTime(self):
        """Get PLC time - mimics pylogix method"""
        try:
            plc = self._get_or_create_plc()
            return plc.get_plc_time()
        except Exception as e:
            logger.error(f"GetPLCTime error: {e}")
            return MockResponse('Error', None)
    
    def Read(self, tag_name):
        """Read a tag - mimics pylogix method"""
        try:
            plc = self._get_or_create_plc()
            return plc.read_tag(tag_name)
        except Exception as e:
            logger.error(f"Read error: {e}")
            return MockResponse('Error', None)
    
    def Write(self, tag_name, value):
        """Write a tag - mimics pylogix method"""
        try:
            plc = self._get_or_create_plc()
            return plc.write_tag(tag_name, value)
        except Exception as e:
            logger.error(f"Write error: {e}")
            return MockResponse('Error', None)
    
    def GetTagList(self):
        """Get tag list - mimics pylogix method"""
        try:
            plc = self._get_or_create_plc()
            return plc.get_tag_list()
        except Exception as e:
            logger.error(f"GetTagList error: {e}")
            return MockResponse('Error', [])
    
    @classmethod
    def stop_all_simulations(cls):
        """Stop all running simulations"""
        for plc in cls._plc_instances.values():
            plc.stop_simulation()
        logger.info("All mock PLC simulations stopped")


# Usage example for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create a mock client
    with MockEthernetIPClient() as client:
        client.IPAddress = "192.168.1.100"
        client.ProcessorSlot = 0
        
        # Test connection
        result = client.GetTagList()
        print(f"Tag List: {result.Status} - {result.Value}")
    
    # Clean up
    MockEthernetIPClient.stop_all_simulations()
