import logging
from datetime import datetime
import threading

logger = logging.getLogger(__name__)

class SNMPService:
    def __init__(self):
        self._connection_status = {}  # Dict to store status per device ID
        self._lock = threading.Lock()
    
    def get_connection_status(self, device_id=None):
        """Get connection status for specific device or all devices"""
        with self._lock:
            if device_id:
                return self._connection_status.get(device_id, {
                    'connected': False,
                    'last_check': None,
                    'message': 'Not connected'
                })
            return self._connection_status
    
    def connect_device(self, config):
        """Establish connection to SNMP device"""
        try:
            from pysnmp.hlapi.v3arch.asyncio import (
                SnmpEngine, CommunityData, 
                UdpTransportTarget, ContextData, ObjectType, ObjectIdentity,
                get_cmd
            )
            import asyncio
            
            async def test_connection():
                try:
                    iterator = get_cmd(
                        SnmpEngine(),
                        CommunityData(config.community),
                        await UdpTransportTarget.create((config.host, config.port), timeout=2, retries=1),
                        ContextData(),
                        ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0))
                    )
                    
                    errorIndication, errorStatus, errorIndex, varBinds = await iterator
                    return errorIndication, errorStatus, errorIndex, varBinds
                except asyncio.TimeoutError:
                    return "Request timeout", None, None, None
            
            # Run async operation with proper cleanup
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                errorIndication, errorStatus, errorIndex, varBinds = loop.run_until_complete(
                    asyncio.wait_for(test_connection(), timeout=5)
                )
            except asyncio.TimeoutError:
                errorIndication = "Connection timeout"
                errorStatus = None
            finally:
                # Cancel all pending tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                # Run loop once more to allow tasks to be cancelled
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
            
            if errorIndication:
                with self._lock:
                    self._connection_status[config.id] = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': str(errorIndication)
                    }
                logger.debug(f"SNMP connection failed for {config.name}: {errorIndication}")
                return False, str(errorIndication)
            elif errorStatus:
                with self._lock:
                    self._connection_status[config.id] = {
                        'connected': False,
                        'last_check': datetime.utcnow(),
                        'message': f'{errorStatus.prettyPrint()} at {errorIndex}'
                    }
                return False, f'{errorStatus.prettyPrint()} at {errorIndex}'
            else:
                with self._lock:
                    self._connection_status[config.id] = {
                        'connected': True,
                        'last_check': datetime.utcnow(),
                        'message': f'Connected to {config.host}'
                    }
                logger.info(f"Connected to SNMP device {config.name} at {config.host}")
                return True, "Connected successfully"
        except Exception as e:
            logger.error(f"SNMP connection failed: {str(e)}")
            with self._lock:
                self._connection_status[config.id] = {
                    'connected': False,
                    'last_check': datetime.utcnow(),
                    'message': str(e)
                }
            return False, str(e)
    
    def discover_objects(self, config, base_oid='1.3.6.1.2.1'):
        """Discover SNMP objects by walking the MIB tree"""
        logger.info(f"Starting SNMP walk for {config.host} with base OID {base_oid}")
        try:
            from pysnmp.hlapi.v3arch.asyncio import (
                SnmpEngine, CommunityData,
                UdpTransportTarget, ContextData, ObjectType, ObjectIdentity,
                next_cmd
            )
            import asyncio
            
            objects = []
            
            async def walk_mib():
                nonlocal objects
                count = 0
                max_objects = 100  # Limit to prevent overwhelming
                
                logger.info(f"Creating SNMP connection to {config.host}:{config.port}")
                
                # Create transport target once
                transport = await UdpTransportTarget.create((config.host, config.port), timeout=5, retries=2)
                snmpEngine = SnmpEngine()
                community = CommunityData(config.community)
                context = ContextData()
                
                # Start with the base OID
                current_oid = ObjectType(ObjectIdentity(base_oid))
                
                try:
                    while count < max_objects:
                        # Use next_cmd with await (not async for)
                        errorIndication, errorStatus, errorIndex, varBinds = await next_cmd(
                            snmpEngine,
                            community,
                            transport,
                            context,
                            current_oid,
                            lexicographicMode=False
                        )
                        
                        if errorIndication:
                            logger.error(f"SNMP error indication: {errorIndication}")
                            break
                        
                        if errorStatus:
                            logger.error(f"SNMP error status: {errorStatus.prettyPrint()}")
                            break
                            
                        for varBind in varBinds:
                            oid_obj = varBind[0]
                            value_obj = varBind[1]
                            
                            oid = str(oid_obj)
                            value = str(value_obj)
                            
                            # Check if we've moved beyond the base OID tree
                            if not oid.startswith(base_oid):
                                logger.info(f"Reached end of OID tree at {oid}")
                                return
                            
                            # Extract MIB metadata from ObjectIdentity
                            try:
                                # Get the label (human-readable name)
                                if hasattr(oid_obj, 'prettyPrint'):
                                    pretty_name = oid_obj.prettyPrint()
                                else:
                                    pretty_name = str(oid_obj)
                                
                                # Extract MIB name from the label if available
                                name = pretty_name.split('::')[-1].split('.')[0] if '::' in pretty_name else f"OID_{oid.split('.')[-1]}"
                                
                                # Get data type from the value object
                                data_type = type(value_obj).__name__
                                if hasattr(value_obj, 'prettyPrint'):
                                    data_type = value_obj.__class__.__name__
                                
                                # Extract description if available from MIB
                                description = f"SNMP OID: {oid}"
                                
                                # Infer access type (read-only by default for walk)
                                access = "read-only"
                                
                                # Status is typically 'current' for discovered objects
                                status = "current"
                                
                            except Exception as e:
                                logger.debug(f"Could not extract full metadata for {oid}: {e}")
                                name = f"OID_{oid.split('.')[-1]}"
                                data_type = "UNKNOWN"
                                description = f"SNMP OID: {oid}"
                                access = "read-only"
                                status = "current"
                            
                            objects.append({
                                'oid': oid,
                                'name': name,
                                'value': value[:50] if len(value) > 50 else value,
                                'data_type': data_type,
                                'description': description,
                                'access': access,
                                'status': status
                            })
                            
                            # Update current OID for next iteration
                            current_oid = ObjectType(ObjectIdentity(oid))
                            
                            count += 1
                            if count >= max_objects:
                                logger.info(f"Reached max objects limit ({max_objects})")
                                return
                
                except asyncio.TimeoutError:
                    logger.warning(f"SNMP walk timeout for {config.host}")
                except Exception as e:
                    logger.error(f"Error during SNMP walk: {str(e)}", exc_info=True)
            
            # Run async operation with proper cleanup
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                logger.info("Running SNMP walk async operation...")
                loop.run_until_complete(asyncio.wait_for(walk_mib(), timeout=15))
            except asyncio.TimeoutError:
                logger.warning(f"SNMP walk timed out after 15 seconds for {config.host}")
            finally:
                # Cancel all pending tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
            
            logger.info(f"✓ Discovered {len(objects)} OIDs from {config.host}")
            return True, objects
        except Exception as e:
            logger.error(f"SNMP OID discovery failed: {str(e)}", exc_info=True)
            return False, []
    
    def read_oid(self, snmp_object):
        try:
            from pysnmp.hlapi.v3arch.asyncio import (
                SnmpEngine, CommunityData, 
                UdpTransportTarget, ContextData, ObjectType, ObjectIdentity,
                get_cmd
            )
            import asyncio
            
            config = snmp_object.config
            
            async def read_value():
                try:
                    iterator = get_cmd(
                        SnmpEngine(),
                        CommunityData(config.community),
                        await UdpTransportTarget.create((config.host, config.port), timeout=2, retries=1),
                        ContextData(),
                        ObjectType(ObjectIdentity(snmp_object.oid))
                    )
                    
                    errorIndication, errorStatus, errorIndex, varBinds = await iterator
                    return errorIndication, errorStatus, errorIndex, varBinds
                except asyncio.TimeoutError:
                    return "Request timeout", None, None, None
            
            # Run async operation with proper cleanup
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                errorIndication, errorStatus, errorIndex, varBinds = loop.run_until_complete(
                    asyncio.wait_for(read_value(), timeout=5)
                )
            except asyncio.TimeoutError:
                errorIndication = "Read timeout"
                errorStatus = None
            finally:
                # Cancel all pending tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
            
            if errorIndication:
                return False, str(errorIndication)
            elif errorStatus:
                return False, f'{errorStatus.prettyPrint()} at {errorIndex}'
            else:
                for varBind in varBinds:
                    return True, varBind[1].prettyPrint()
                return False, "No value returned"
                
        except Exception as e:
            logger.error(f"OID read failed: {str(e)}")
            return False, str(e)
    
    def walk_oid(self, config, oid):
        try:
            from pysnmp.hlapi import (
                nextCmd, SnmpEngine, CommunityData, 
                UdpTransportTarget, ContextData, ObjectType, ObjectIdentity
            )
            
            results = []
            
            for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
                SnmpEngine(),
                CommunityData(config.community),
                UdpTransportTarget((config.host, config.port), timeout=2, retries=1),
                ContextData(),
                ObjectType(ObjectIdentity(oid)),
                lexicographicMode=False
            ):
                if errorIndication:
                    return False, str(errorIndication)
                elif errorStatus:
                    return False, f'{errorStatus.prettyPrint()} at {errorIndex}'
                else:
                    for varBind in varBinds:
                        results.append({
                            'oid': varBind[0].prettyPrint(),
                            'value': varBind[1].prettyPrint()
                        })
            
            return True, results
                
        except Exception as e:
            logger.error(f"SNMP walk failed: {str(e)}")
            return False, str(e)
    
    def write_oid(self, config, oid, value, data_type='INTEGER'):
        """Write value to SNMP OID"""
        try:
            from pysnmp.hlapi.v3arch.asyncio import (
                SnmpEngine, CommunityData, 
                UdpTransportTarget, ContextData, ObjectType, ObjectIdentity,
                set_cmd
            )
            from pysnmp.proto import rfc1902
            import asyncio
            
            # Convert value to appropriate SNMP type
            snmp_value = None
            try:
                if data_type.upper() in ['INTEGER', 'INT', 'COUNTER32', 'GAUGE32']:
                    snmp_value = rfc1902.Integer32(int(value))
                elif data_type.upper() in ['STRING', 'OCTETSTRING', 'DISPLAYSTRING']:
                    snmp_value = rfc1902.OctetString(str(value))
                elif data_type.upper() in ['COUNTER64']:
                    snmp_value = rfc1902.Counter64(int(value))
                elif data_type.upper() in ['UNSIGNED32']:
                    snmp_value = rfc1902.Unsigned32(int(value))
                elif data_type.upper() in ['IPADDRESS']:
                    snmp_value = rfc1902.IpAddress(str(value))
                else:
                    # Default to OctetString for unknown types
                    snmp_value = rfc1902.OctetString(str(value))
                    
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to convert value '{value}' to SNMP type '{data_type}': {str(e)}")
                return False, f"Invalid value for data type {data_type}"
            
            async def write_value():
                try:
                    iterator = set_cmd(
                        SnmpEngine(),
                        CommunityData(config.community),
                        await UdpTransportTarget.create((config.host, config.port), timeout=5, retries=2),
                        ContextData(),
                        ObjectType(ObjectIdentity(oid), snmp_value)
                    )
                    
                    errorIndication, errorStatus, errorIndex, varBinds = await iterator
                    return errorIndication, errorStatus, errorIndex, varBinds
                except asyncio.TimeoutError:
                    return "Write timeout", None, None, None
            
            # Run async operation with proper cleanup
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                errorIndication, errorStatus, errorIndex, varBinds = loop.run_until_complete(
                    asyncio.wait_for(write_value(), timeout=8)
                )
            except asyncio.TimeoutError:
                errorIndication = "Write timeout"
                errorStatus = None
            finally:
                # Cancel all pending tasks
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
            
            if errorIndication:
                logger.error(f"SNMP write failed for OID {oid}: {errorIndication}")
                return False, str(errorIndication)
            elif errorStatus:
                logger.error(f"SNMP write error for OID {oid}: {errorStatus.prettyPrint()}")
                return False, f'{errorStatus.prettyPrint()} at {errorIndex}'
            else:
                logger.info(f"Successfully wrote value '{value}' to OID {oid} on {config.host}")
                return True, "Write successful"
                
        except Exception as e:
            logger.error(f"SNMP write operation failed: {str(e)}")
            return False, str(e)

    def write_by_name(self, config, parameter_name, value):
        """Write value to SNMP object by parameter name (finds OID by name)"""
        try:
            from models import SNMPObject
            from database import db
            
            # Find the SNMP object by name and config
            snmp_object = db.session.query(SNMPObject).filter_by(
                config_id=config.id, 
                name=parameter_name
            ).first()
            
            if not snmp_object:
                logger.warning(f"SNMP object with name '{parameter_name}' not found for config {config.id}")
                return False, f"Parameter '{parameter_name}' not found"
            
            # Check if the object is writable
            if snmp_object.access and 'write' not in snmp_object.access.lower():
                logger.warning(f"SNMP object '{parameter_name}' is not writable (access: {snmp_object.access})")
                return False, f"Parameter '{parameter_name}' is read-only"
            
            # Write to the OID
            success, message = self.write_oid(config, snmp_object.oid, value, snmp_object.data_type or 'STRING')
            
            if success:
                # Update last_value in database
                snmp_object.last_value = str(value)
                snmp_object.last_read = datetime.utcnow()
                db.session.commit()
            
            return success, message
            
        except Exception as e:
            logger.error(f"Failed to write by name '{parameter_name}': {str(e)}")
            return False, str(e)
    
    def detect_devices(self, ip_range, port=161, community='public', version='v2c', timeout=3):
        """Detect SNMP devices in a given IP range"""
        import ipaddress
        import threading
        import asyncio
        from pysnmp.hlapi.v3arch.asyncio import (
            SnmpEngine, CommunityData,
            UdpTransportTarget, ContextData, ObjectType, ObjectIdentity,
            get_cmd
        )
        
        try:
            network = ipaddress.ip_network(ip_range, strict=False)
            devices = []
            lock = threading.Lock()
            
            def check_device(ip):
                try:
                    async def test_snmp():
                        try:
                            iterator = get_cmd(
                                SnmpEngine(),
                                CommunityData(community),
                                await UdpTransportTarget.create((str(ip), port), timeout=timeout, retries=1),
                                ContextData(),
                                ObjectType(ObjectIdentity('SNMPv2-MIB', 'sysDescr', 0))
                            )
                            
                            errorIndication, errorStatus, errorIndex, varBinds = await iterator
                            return errorIndication, errorStatus, errorIndex, varBinds
                        except:
                            return "error", None, None, None
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        errorIndication, errorStatus, errorIndex, varBinds = loop.run_until_complete(
                            asyncio.wait_for(test_snmp(), timeout=timeout + 1)
                        )
                    except asyncio.TimeoutError:
                        errorIndication = "Timeout"
                        errorStatus = None
                    finally:
                        pending = asyncio.all_tasks(loop)
                        for task in pending:
                            task.cancel()
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                        loop.close()
                    
                    if not errorIndication and not errorStatus:
                        with lock:
                            devices.append({
                                'host': str(ip),
                                'port': port,
                                'community': community,
                                'version': version,
                                'polling_interval': 5000
                            })
                        logger.info(f"Found SNMP device at {ip}")
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
                        t.join(timeout=timeout + 2)
                    threads = [t for t in threads if t.is_alive()]
            
            # Wait for remaining threads
            for thread in threads:
                thread.join(timeout=timeout + 2)
            
            logger.info(f"SNMP device detection completed. Found {len(devices)} devices")
            return True, devices
            
        except ValueError as e:
            logger.error(f"Invalid IP range: {str(e)}")
            return False, f"Invalid IP range: {str(e)}"
        except Exception as e:
            logger.error(f"Device detection failed: {str(e)}")
            return False, str(e)
