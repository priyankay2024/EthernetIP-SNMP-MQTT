from services.ethernetip_service import EthernetIPService
from services.snmp_service import SNMPService
from services.mqtt_service import MQTTService
from services.data_logging_service import DataLoggingService
from services.polling_service import PollingService

ethernetip_service = EthernetIPService()
snmp_service = SNMPService()
mqtt_service = MQTTService()
data_logging_service = DataLoggingService()

# Polling service will be initialized in app.py after db is ready
polling_service = None
