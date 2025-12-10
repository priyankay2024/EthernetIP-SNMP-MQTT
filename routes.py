"""
Protocol Bridge Routes
Main application routes using Blueprint pattern
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from datetime import datetime
from database import db
from models import (EthernetIPConfig, SNMPConfig, MQTTConfig, EthernetIPTag, SNMPObject,
                    DataLog, TagMapping, PollingSchedule, MQTTSubscription)
import logging
from json import loads as json_loads, JSONDecodeError

logger = logging.getLogger(__name__)

# Create Blueprint
main_bp = Blueprint('main', __name__)


def get_services():
    """Get service instances from current app"""
    ethernetip_service = current_app.config.get('ethernetip_service')
    snmp_service = current_app.config.get('snmp_service')
    mqtt_service = current_app.config.get('mqtt_service')
    data_logging_service = current_app.config.get('data_logging_service')
    polling_service = current_app.config.get('polling_service')
    return ethernetip_service, snmp_service, mqtt_service, data_logging_service, polling_service


@main_bp.route('/')
def dashboard():
    eip_configs = db.session.query(EthernetIPConfig).filter_by(enabled=True).all()
    snmp_configs = db.session.query(SNMPConfig).filter_by(enabled=True).all()
    mqtt_configs = db.session.query(MQTTConfig).filter_by(enabled=True).all()
    
    eip_service, snmp_service, mqtt_service, _, polling_service = get_services()
    
    # Calculate overall connection status
    eip_connected_count = 0
    if eip_service:
        all_eip_status = eip_service.get_connection_status()
        eip_connected_count = sum(1 for status in all_eip_status.values() if status.get('connected', False))
    
    snmp_connected_count = 0
    if snmp_service:
        all_snmp_status = snmp_service.get_connection_status()
        snmp_connected_count = sum(1 for status in all_snmp_status.values() if status.get('connected', False))
    
    mqtt_connected_count = 0
    if mqtt_service:
        all_mqtt_status = mqtt_service.get_connection_status()
        mqtt_connected_count = sum(1 for status in all_mqtt_status.values() if status.get('connected', False))
    
    eip_status = {
        'connected': eip_connected_count > 0,
        'message': f'{eip_connected_count} of {len(eip_configs)} devices connected'
    }
    snmp_status = {
        'connected': snmp_connected_count > 0,
        'message': f'{snmp_connected_count} of {len(snmp_configs)} devices connected'
    }
    mqtt_status = {
        'connected': mqtt_connected_count > 0,
        'message': f'{mqtt_connected_count} of {len(mqtt_configs)} brokers connected'
    }
    
    tag_count = db.session.query(EthernetIPTag).filter_by(enabled=True).count()
    object_count = db.session.query(SNMPObject).filter_by(enabled=True).count()
    
    # Get polling service status
    polling_status = {
        'running': False,
        'message': 'Polling service not available'
    }
    if polling_service:
        status = polling_service.get_status()
        polling_status = {
            'running': status.get('running', False),
            'message': 'Active - Collecting data' if status.get('running') else 'Inactive'
        }
    
    return render_template('dashboard.html', 
                          eip_configs=eip_configs,
                          snmp_configs=snmp_configs,
                          mqtt_configs=mqtt_configs,
                          eip_status=eip_status,
                          snmp_status=snmp_status,
                          mqtt_status=mqtt_status,
                          polling_status=polling_status,
                          tag_count=tag_count,
                          object_count=object_count)

@main_bp.route('/config')
def config():
    return redirect(url_for('main.config_ethernetip'))

@main_bp.route('/config/ethernetip', methods=['GET', 'POST'])
def config_ethernetip():
    if request.method == 'POST':
        logger.info("=== POST request to config_ethernetip ===")
        logger.info(f"Form data: {dict(request.form)}")
        
        action = request.form.get('action')
        logger.info(f"Action: {action}")
        eip_service, _, _, _, _ = get_services()
        
        if action == 'add':
            logger.info("Adding new device...")
            config = EthernetIPConfig(
                name=request.form.get('name', 'Default'),
                ip_address=request.form.get('ip_address'),
                slot=int(request.form.get('slot', 0)),
                timeout=float(request.form.get('timeout', 5.0)),
                hwid=request.form.get('hwid') or None,
                polling_interval=int(request.form.get('polling_interval', 1000)),
                description=request.form.get('description') or None,
                enabled=True  # Always enable new devices
            )
            db.session.add(config)
            db.session.commit()
            logger.info(f"Device saved with ID: {config.id}, Name: {config.name}, Enabled: {config.enabled}")
            
            # Save selected tags
            selected_tags_json = request.form.get('selected_tags')
            logger.info(f"Selected tags JSON: {selected_tags_json}")
            
            if selected_tags_json:
                try:
                    selected_tags = json_loads(selected_tags_json)
                    logger.info(f"Parsed selected tags: {selected_tags}")
                    
                    # Discover tags to get their data types
                    if eip_service:
                        success, result = eip_service.discover_tags(config)
                        logger.info(f"Discovery result: success={success}, tags count={len(result) if success else 0}")
                        
                        if success:
                            tag_map = {tag['name']: tag['data_type'] for tag in result}
                            logger.info(f"Tag map created with {len(tag_map)} tags")
                            
                            for tag_name in selected_tags:
                                tag = EthernetIPTag(
                                    config_id=config.id,
                                    tag_name=tag_name,
                                    data_type=tag_map.get(tag_name, 'UNKNOWN'),
                                    enabled=True
                                )
                                db.session.add(tag)
                                logger.info(f"Added tag: {tag_name} with type {tag_map.get(tag_name, 'UNKNOWN')}")
                            
                            db.session.commit()
                            logger.info(f"✓ Saved {len(selected_tags)} tags for device {config.name}")
                        else:
                            logger.warning(f"Failed to discover tags for validation: {result}")
                except JSONDecodeError as e:
                    logger.error(f"Failed to parse selected_tags JSON: {e}")
                except Exception as e:
                    logger.error(f"Error saving tags: {e}", exc_info=True)
            else:
                logger.warning("No selected_tags in form data")
            
            # Auto-connect the device
            if eip_service:
                success, message = eip_service.connect_device(config)
                if success:
                    flash(f'Device added and connected: {config.name}', 'success')
                else:
                    flash(f'Device added but connection failed: {message}', 'warning')
            else:
                flash('EthernetIP configuration added successfully', 'success')
            
            return redirect(url_for('main.config_ethernetip'))
        
        elif action == 'update':
            config_id = request.form.get('config_id')
            config = EthernetIPConfig.query.get(config_id)
            if config:
                config.name = request.form.get('name', 'Default')
                config.ip_address = request.form.get('ip_address')
                config.slot = int(request.form.get('slot', 0))
                config.timeout = float(request.form.get('timeout', 5.0))
                config.hwid = request.form.get('hwid') or None
                config.polling_interval = int(request.form.get('polling_interval', 1000))
                config.description = request.form.get('description') or None
                config.enabled = request.form.get('enabled', 'on') == 'on'
                
                # Handle tag management
                selected_tags_json = request.form.get('selected_tags')
                if selected_tags_json:
                    try:
                        selected_tags = json_loads(selected_tags_json)
                        
                        # Get existing tags for this device
                        existing_tags = {tag.tag_name: tag for tag in EthernetIPTag.query.filter_by(config_id=config_id).all()}
                        
                        # Enable selected tags and add new ones
                        for tag_name in selected_tags:
                            if tag_name in existing_tags:
                                # Re-enable if disabled
                                existing_tags[tag_name].enabled = True
                            else:
                                # Add new tag - data_type will be discovered on next poll
                                new_tag = EthernetIPTag(
                                    config_id=config_id,
                                    tag_name=tag_name,
                                    data_type='Unknown',  # Will be updated on discovery
                                    enabled=True
                                )
                                db.session.add(new_tag)
                        
                        # Disable tags that are not selected
                        for tag_name, tag in existing_tags.items():
                            if tag_name not in selected_tags:
                                tag.enabled = False
                    except JSONDecodeError:
                        flash('Invalid tag selection data', 'error')
                
                db.session.commit()
                
                # Reconnect if enabled
                if config.enabled and eip_service:
                    eip_service.connect_device(config)
                
                flash('EthernetIP configuration updated successfully', 'success')
        
        elif action == 'delete':
            config_id = request.form.get('config_id')
            config = EthernetIPConfig.query.get(config_id)
            if config:
                # Delete all associated tags first
                EthernetIPTag.query.filter_by(config_id=config_id).delete()
                # Then delete the device
                db.session.delete(config)
                db.session.commit()
                flash('EthernetIP configuration deleted successfully', 'success')
        
        elif action == 'test':
            config_id = request.form.get('config_id')
            config = EthernetIPConfig.query.get(config_id)
            if config and eip_service:
                success, message = eip_service.connect_device(config)
                if success:
                    flash(f'Connection successful: {message}', 'success')
                else:
                    flash(f'Connection failed: {message}', 'error')
        
        return redirect(url_for('main.config_ethernetip'))
    
    # GET request with pagination and filtering
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    search_device = request.args.get('device', '')
    
    # Build query with optional filtering
    query = db.session.query(EthernetIPConfig)
    
    if search_device:
        if '|' in search_device:
            name_part, ip_part = search_device.split('|', 1)
            query = query.filter(
                db.or_(
                    EthernetIPConfig.name == name_part,
                    EthernetIPConfig.ip_address == ip_part
                )
            )
        else:
            query = query.filter(
                db.or_(
                    EthernetIPConfig.name.ilike(f'%{search_device}%'),
                    EthernetIPConfig.ip_address.ilike(f'%{search_device}%')
                )
            )
    
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get all devices for filter dropdown
    all_devices = db.session.query(EthernetIPConfig).all()
    
    # Get connection status for each device
    eip_service, _, _, _, _ = get_services()
    device_status = {}
    if eip_service:
        for config in pagination.items:
            status = eip_service.get_connection_status(config.id)
            device_status[config.id] = status
    
    return render_template('config_ethernetip.html', 
                         configs=pagination.items,
                         pagination=pagination,
                         all_devices=all_devices,
                         search_device=search_device,
                         device_status=device_status,
                         active_tab='ethernetip')


@main_bp.route('/api/ethernetip/discover-tags', methods=['POST'])
def discover_ethernetip_tags():
    """API endpoint to discover tags from an EthernetIP device"""
    logger.info("=== Discover Tags Endpoint Called ===")
    try:
        data = request.get_json()
        logger.info(f"Request data: {data}")
        config_id = data.get('config_id')
        
        # Get service
        eip_service, _, _, _, _ = get_services()
        logger.info(f"EthernetIP service retrieved: {eip_service}")
        logger.info(f"Service type: {type(eip_service)}")
        
        if not eip_service:
            logger.error("EthernetIP service is None!")
            return jsonify({'success': False, 'message': 'Service not available'}), 500
        
        # If config_id provided, get existing config
        if config_id:
            config = EthernetIPConfig.query.get(config_id)
            if not config:
                return jsonify({'success': False, 'message': 'Config not found'}), 404
        else:
            # Create temporary config for new device discovery
            ip_address = data.get('ip_address')
            slot = int(data.get('slot', 0))
            timeout = float(data.get('timeout', 5.0))
            
            logger.info(f"Creating temp config: IP={ip_address}, Slot={slot}, Timeout={timeout}")
            
            if not ip_address:
                return jsonify({'success': False, 'message': 'IP address required'}), 400
            
            config = EthernetIPConfig(
                ip_address=ip_address,
                slot=slot,
                timeout=timeout
            )
        
        logger.info(f"Calling discover_tags on service...")
        success, result = eip_service.discover_tags(config)
        logger.info(f"Discovery result: success={success}, result={result}")
        
        if success:
            response_data = {'success': True, 'tags': result}
            
            # If config_id provided, include existing tags
            if config_id:
                existing_tags = EthernetIPTag.query.filter_by(config_id=config_id).all()
                response_data['existing_tags'] = [
                    {
                        'tag_name': tag.tag_name,
                        'data_type': tag.data_type,
                        'enabled': tag.enabled
                    } for tag in existing_tags
                ]
            
            return jsonify(response_data)
        else:
            return jsonify({'success': False, 'message': result}), 400
            
    except Exception as e:
        logger.error(f"Tag discovery error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'message': str(e)}), 500


@main_bp.route('/api/ethernetip/connection-status/<int:config_id>')
def get_ethernetip_status(config_id):
    """API endpoint to get real-time connection status"""
    eip_service, _, _, _, _ = get_services()
    if eip_service:
        status = eip_service.get_connection_status(config_id)
        return jsonify(status)
    return jsonify({'success': False, 'connected': False, 'message': 'Service not available'})


@main_bp.route('/api/ethernetip/device-tags/<int:config_id>')
def get_device_tags(config_id):
    """API endpoint to get saved tags for a device"""
    try:
        tags = EthernetIPTag.query.filter_by(config_id=config_id).all()
        tags_list = [{
            'id': tag.id,
            'tag_name': tag.tag_name,
            'data_type': tag.data_type or 'UNKNOWN',
            'enabled': tag.enabled,
            'last_value': tag.last_value,
            'last_read': tag.last_read.isoformat() if tag.last_read else None
        } for tag in tags]
        return jsonify({'success': True, 'tags': tags_list})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@main_bp.route('/config/snmp', methods=['GET', 'POST'])
def config_snmp():
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search_device = request.args.get('device', '')
    
    if request.method == 'POST':
        action = request.form.get('action')
        _, snmp_service, _, _, _ = get_services()
        
        if action == 'add':
            config = SNMPConfig(
                name=request.form.get('name', 'Default'),
                host=request.form.get('host'),
                port=int(request.form.get('port', 161)),
                community=request.form.get('community', 'public'),
                version=request.form.get('version', 'v2c'),
                hwid=request.form.get('hwid'),
                polling_interval=int(request.form.get('polling_interval', 5000)),
                enabled=True  # Always enable new devices
            )
            db.session.add(config)
            db.session.commit()
            logger.info(f"SNMP device saved with ID: {config.id}, Name: {config.name}")
            
            # Save selected OIDs
            selected_oids_json = request.form.get('selected_oids')
            logger.info(f"Selected OIDs JSON: {selected_oids_json}")
            
            if selected_oids_json:
                try:
                    selected_oids = json_loads(selected_oids_json)
                    logger.info(f"Parsed selected OIDs: {len(selected_oids)} items")
                    
                    for oid_data in selected_oids:
                        snmp_obj = SNMPObject(
                            config_id=config.id,
                            oid=oid_data['oid'],
                            name=oid_data.get('name', f"OID_{oid_data['oid'].split('.')[-1]}"),
                            description=oid_data.get('description'),
                            data_type=oid_data.get('data_type', 'UNKNOWN'),
                            access=oid_data.get('access', 'read-only'),
                            status=oid_data.get('status', 'current'),
                            poll_rate=5000,
                            enabled=True
                        )
                        db.session.add(snmp_obj)
                        logger.info(f"Added OID: {oid_data['oid']} ({oid_data.get('name')})")
                    
                    db.session.commit()
                    logger.info(f"✓ Saved {len(selected_oids)} OIDs for device {config.name}")
                except JSONDecodeError as e:
                    logger.error(f"Failed to parse selected_oids JSON: {e}")
                except Exception as e:
                    logger.error(f"Error saving OIDs: {e}", exc_info=True)
            else:
                logger.warning("No selected_oids in form data")
            
            # Auto-connect the device
            if snmp_service:
                success, message = snmp_service.connect_device(config)
                if success:
                    flash(f'SNMP device added and connected: {config.name}', 'success')
                else:
                    flash(f'SNMP device added but connection failed: {message}', 'warning')
            else:
                flash('SNMP device added successfully', 'success')

        
        elif action == 'update':
            config_id = request.form.get('config_id')
            config = SNMPConfig.query.get(config_id)
            if config:
                config.name = request.form.get('name', 'Default')
                config.host = request.form.get('host')
                config.port = int(request.form.get('port', 161))
                config.hwid = request.form.get('hwid')
                config.community = request.form.get('community', 'public')
                config.version = request.form.get('version', 'v2c')
                config.polling_interval = int(request.form.get('polling_interval', 5000))
                config.enabled = request.form.get('enabled') == 'on'
                
                # Handle OID management
                selected_oids_json = request.form.get('selected_oids')
                if selected_oids_json:
                    try:
                        selected_oids = json_loads(selected_oids_json)  # Array of OID strings
                        
                        # Get existing objects for this device
                        existing_objects = {obj.oid: obj for obj in SNMPObject.query.filter_by(config_id=config_id).all()}
                        
                        # Enable selected OIDs and add new ones
                        for oid in selected_oids:
                            if oid in existing_objects:
                                # Re-enable if disabled
                                existing_objects[oid].enabled = True
                            else:
                                # Add new OID - will be populated with details on next poll
                                new_obj = SNMPObject(
                                    config_id=config_id,
                                    oid=oid,
                                    name=f"OID_{oid.split('.')[-1]}",
                                    data_type='UNKNOWN',
                                    access='read-only',
                                    status='current',
                                    poll_rate=config.polling_interval,
                                    enabled=True
                                )
                                db.session.add(new_obj)
                        
                        # Disable OIDs that are not selected
                        for oid, obj in existing_objects.items():
                            if oid not in selected_oids:
                                obj.enabled = False
                    except JSONDecodeError:
                        flash('Invalid OID selection data', 'error')
                
                db.session.commit()
                flash('SNMP configuration updated successfully', 'success')
        
        elif action == 'delete':
            config_id = request.form.get('config_id')
            config = SNMPConfig.query.get(config_id)
            if config:
                # Delete all associated objects first
                SNMPObject.query.filter_by(config_id=config_id).delete()
                # Then delete the device
                db.session.delete(config)
                db.session.commit()
                flash('SNMP configuration deleted successfully', 'success')
        
        return redirect(url_for('main.config_snmp'))
    
    # Build query with filters
    query = SNMPConfig.query
    if search_device:
        # Handle combined "name|host" format from dropdown
        if '|' in search_device:
            # Split combined value and match exactly
            name_part, host_part = search_device.split('|', 1)
            query = query.filter(
                db.or_(
                    SNMPConfig.name == name_part,
                    SNMPConfig.host == host_part
                )
            )
        else:
            # Search both name and host with the same term (partial match)
            query = query.filter(
                db.or_(
                    SNMPConfig.name.ilike(f'%{search_device}%'),
                    SNMPConfig.host.ilike(f'%{search_device}%')
                )
            )
    
    configs_pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get all devices for dropdown
    all_devices = SNMPConfig.query.order_by(SNMPConfig.name).all()
    
    return render_template('config_snmp.html', 
                         configs=configs_pagination.items,
                         pagination=configs_pagination,
                         search_device=search_device,
                         all_devices=all_devices,
                         active_tab='snmp')

@main_bp.route('/api/snmp/discover-objects', methods=['POST'])
def snmp_discover_objects():
    """Discover SNMP objects from a device"""
    try:
        config_id = request.json.get('config_id')
        base_oid = request.json.get('base_oid', '1.3.6.1.2.1')  # Default to MIB-2
        
        logger.info(f"Discovering SNMP OIDs for config_id={config_id}, base_oid={base_oid}")
        
        config = SNMPConfig.query.get(config_id)
        if not config:
            logger.warning(f"SNMP config {config_id} not found")
            return jsonify({'success': False, 'message': 'Configuration not found'}), 404
        
        _, snmp_service, _, _, _ = get_services()
        if not snmp_service:
            logger.error("SNMP service not available")
            return jsonify({'success': False, 'message': 'SNMP service not available'}), 503
        
        logger.info(f"Starting OID discovery for {config.name} ({config.host})")
        objects = snmp_service.discover_objects(config, base_oid)
        logger.info(f"Discovery completed: {len(objects)} OIDs found")
        
        response_data = {
            'success': True,
            'objects': objects,
            'count': len(objects)
        }
        
        # Include existing objects
        existing_objects = SNMPObject.query.filter_by(config_id=config_id).all()
        response_data['existing_objects'] = [
            {
                'oid': obj.oid,
                'name': obj.name,
                'enabled': obj.enabled
            } for obj in existing_objects
        ]
        
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Error in SNMP discovery: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Discovery failed: {str(e)}'
        }), 500

@main_bp.route('/api/snmp/discover-objects-temp', methods=['POST'])
def snmp_discover_objects_temp():
    """Discover SNMP objects from a temporary (unsaved) device configuration"""
    try:
        data = request.json
        host = data.get('host')
        port = data.get('port', 161)
        community = data.get('community', 'public')
        version = data.get('version', 'v2c')
        base_oid = data.get('base_oid', '1.3.6.1.2.1')
        
        if not host:
            return jsonify({'success': False, 'message': 'Host is required'}), 400
        
        logger.info(f"Discovering SNMP OIDs for temporary config: {host}:{port}")
        
        # Create temporary config object (not saved to database)
        temp_config = SNMPConfig(
            name='Temporary',
            host=host,
            port=port,
            community=community,
            version=version
        )
        
        _, snmp_service, _, _, _ = get_services()
        if not snmp_service:
            logger.error("SNMP service not available")
            return jsonify({'success': False, 'message': 'SNMP service not available'}), 503
        
        logger.info(f"Starting OID discovery for temp device ({host})")
        objects = snmp_service.discover_objects(temp_config, base_oid)
        logger.info(f"Discovery completed: {len(objects)} OIDs found")
        
        return jsonify({
            'success': True,
            'objects': objects,
            'count': len(objects)
        })
    except Exception as e:
        logger.error(f"Error in temporary SNMP discovery: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Discovery failed: {str(e)}'
        }), 500

@main_bp.route('/api/snmp/connection-status/<int:config_id>')
def snmp_connection_status(config_id):
    """Get real-time connection status for a specific SNMP device"""
    _, snmp_service, _, _, _ = get_services()
    
    if snmp_service:
        status = snmp_service.get_connection_status(config_id)
        return jsonify({
            'success': True,
            'connected': status['connected'],
            'last_check': status['last_check'].isoformat() if status['last_check'] else None,
            'message': status['message']
        })
    
    return jsonify({
        'success': False,
        'message': 'SNMP service not available'
    }), 503

@main_bp.route('/config/mqtt', methods=['GET', 'POST'])
def config_mqtt():
    if request.method == 'POST':
        action = request.form.get('action')
        _, _, mqtt_service, _, _ = get_services()
        
        if action == 'add':
            config = MQTTConfig(
                name=request.form.get('name', 'Default'),
                broker=request.form.get('broker'),
                port=int(request.form.get('port', 1883)),
                username=request.form.get('username') or None,
                password=request.form.get('password') or None,
                topic_prefix=request.form.get('topic_prefix', 'bridge'),
                publish_format=request.form.get('publish_format', 'json'),
                use_tls=request.form.get('use_tls') == 'on',
                enabled=True  # Always enable new devices
            )
            db.session.add(config)
            db.session.commit()
            
            # Auto-connect the broker
            if mqtt_service:
                success, message = mqtt_service.connect_broker(config)
                if success:
                    flash(f'MQTT broker added and connected: {config.name}', 'success')
                else:
                    flash(f'MQTT broker added but connection failed: {message}', 'warning')
            else:
                flash('MQTT broker added successfully', 'success')
        
        elif action == 'update':
            config_id = request.form.get('config_id')
            config = MQTTConfig.query.get(config_id)
            if config:
                config.name = request.form.get('name', 'Default')
                config.broker = request.form.get('broker')
                config.port = int(request.form.get('port', 1883))
                config.username = request.form.get('username') or None
                config.password = request.form.get('password') or None
                config.topic_prefix = request.form.get('topic_prefix', 'bridge')
                config.publish_format = request.form.get('publish_format', 'json')
                config.use_tls = request.form.get('use_tls') == 'on'
                config.enabled = request.form.get('enabled') == 'on'
                db.session.commit()
                flash('MQTT configuration updated successfully', 'success')
        
        elif action == 'delete':
            config_id = request.form.get('config_id')
            config = MQTTConfig.query.get(config_id)
            if config:
                db.session.delete(config)
                db.session.commit()
                flash('MQTT configuration deleted successfully', 'success')
        
        return redirect(url_for('main.config_mqtt'))
    
    # GET request with pagination and filtering
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    search_broker = request.args.get('broker', '')
    
    # Build query with optional filtering
    query = db.session.query(MQTTConfig)
    
    if search_broker:
        query = query.filter(MQTTConfig.name.ilike(f'%{search_broker}%'))
    
    configs_pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    # Get all brokers for filter dropdown
    all_brokers = db.session.query(MQTTConfig).all()
    
    return render_template('config_mqtt.html', 
                         configs=configs_pagination.items,
                         pagination=configs_pagination,
                         all_brokers=all_brokers,
                         search_broker=search_broker,
                         active_tab='mqtt')

@main_bp.route('/api/mqtt/connection-status/<int:config_id>')
def mqtt_connection_status(config_id):
    """Get real-time connection status for a specific MQTT broker"""
    _, _, mqtt_service, _, _ = get_services()
    
    if mqtt_service:
        status = mqtt_service.get_connection_status(config_id)
        return jsonify({
            'success': True,
            'connected': status['connected'],
            'last_check': status['last_check'].isoformat() if status['last_check'] else None,
            'message': status['message']
        })
    
    return jsonify({
        'success': False,
        'message': 'MQTT service not available'
    }), 503

@main_bp.route('/tags', methods=['GET'])
def tags():
    # Get search parameters
    search_device = request.args.get('device', '')
    search_tag = request.args.get('tag', '')
    page = request.args.get('page', 1, type=int)
    per_page = 10
    
    # Build query with joins
    query = db.session.query(EthernetIPTag).join(EthernetIPConfig)
    
    # Apply filters
    if search_device:
        query = query.filter(EthernetIPConfig.id == int(search_device))
    if search_tag:
        query = query.filter(EthernetIPTag.tag_name.ilike(f'%{search_tag}%'))
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    tags = pagination.items
    
    # Get all devices for dropdown
    all_devices = db.session.query(EthernetIPConfig).order_by(EthernetIPConfig.name).all()
    
    # Get all unique tag names for dropdown
    all_tag_names = db.session.query(EthernetIPTag.tag_name).distinct().order_by(EthernetIPTag.tag_name).all()
    all_tag_names = [name[0] for name in all_tag_names]
    
    return render_template('tags.html', 
                          tags=tags, 
                          pagination=pagination,
                          search_device=search_device,
                          search_tag=search_tag,
                          all_devices=all_devices,
                          all_tag_names=all_tag_names,
                          now=datetime.utcnow)

@main_bp.route('/objects', methods=['GET', 'POST'])
def objects():
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            obj = SNMPObject(
                config_id=int(request.form.get('config_id')),
                oid=request.form.get('oid'),
                name=request.form.get('name'),
                description=request.form.get('description'),
                data_type=request.form.get('data_type'),
                access=request.form.get('access', 'read-only'),
                status=request.form.get('status', 'current'),
                poll_rate=int(request.form.get('poll_rate', 5000)),
                enabled=request.form.get('enabled') == 'on'
            )
            db.session.add(obj)
            db.session.commit()
            flash('SNMP Object added successfully', 'success')
        
        elif action == 'update':
            obj_id = request.form.get('object_id')
            obj = SNMPObject.query.get(obj_id)
            if obj:
                obj.config_id = int(request.form.get('config_id'))
                obj.oid = request.form.get('oid')
                obj.name = request.form.get('name')
                obj.description = request.form.get('description')
                obj.data_type = request.form.get('data_type')
                obj.access = request.form.get('access', 'read-only')
                obj.status = request.form.get('status', 'current')
                obj.poll_rate = int(request.form.get('poll_rate', 5000))
                obj.enabled = request.form.get('enabled') == 'on'
                db.session.commit()
                flash('SNMP Object updated successfully', 'success')
        
        elif action == 'delete':
            obj_id = request.form.get('object_id')
            obj = SNMPObject.query.get(obj_id)
            if obj:
                db.session.delete(obj)
                db.session.commit()
                flash('SNMP Object deleted successfully', 'success')
        
        elif action == 'read':
            object_id = request.form.get('object_id')
            obj = SNMPObject.query.get(object_id)
            if obj:
                _, snmp_service, _, data_log_service, _ = get_services()
                if snmp_service:
                    success, value = snmp_service.read_oid(obj)
                    if success:
                        obj.last_value = str(value)
                        obj.last_read = datetime.utcnow()
                        db.session.commit()
                        if data_log_service:
                            data_log_service.log_value('snmp', obj.id, obj.name or obj.oid, value)
                        flash(f'Object read successfully: {value}', 'success')
                    else:
                        flash(f'Failed to read object: {value}', 'error')
        
        return redirect(url_for('main.objects'))
    
    # GET request with pagination and filtering
    page = request.args.get('page', 1, type=int)
    per_page = 10
    search_device = request.args.get('device', '')
    search_object = request.args.get('object', '')
    
    # Build query with joins
    query = db.session.query(SNMPObject).join(SNMPConfig)
    
    # Apply filters
    if search_device:
        query = query.filter(SNMPConfig.id == int(search_device))
    if search_object:
        query = query.filter(SNMPObject.name.ilike(f'%{search_object}%'))
    
    # Paginate
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    all_objects = pagination.items
    
    # Get all devices and objects for dropdowns
    configs = SNMPConfig.query.order_by(SNMPConfig.name).all()
    all_object_names = db.session.query(SNMPObject.name).distinct().order_by(SNMPObject.name).all()
    all_object_names = [name[0] for name in all_object_names if name[0]]
    
    return render_template('objects.html', 
                         objects=all_objects, 
                         configs=configs,
                         pagination=pagination,
                         search_device=search_device,
                         search_object=search_object,
                         all_object_names=all_object_names,
                         now=datetime.utcnow)


@main_bp.route('/logs')
def logs():
    _, _, _, data_log_service, _ = get_services()
    recent_logs = data_log_service.get_recent_logs(limit=200) if data_log_service else []
    return render_template('logs.html', logs=recent_logs)


@main_bp.route('/api/chart-data/<source_type>/<int:source_id>')
def chart_data(source_type, source_id):
    _, _, _, data_log_service, _ = get_services()
    hours = request.args.get('hours', 24, type=int)
    data = data_log_service.get_chart_data(source_type, source_id, hours=hours) if data_log_service else []
    return jsonify(data)


@main_bp.route('/api/logs/recent')
def api_recent_logs():
    _, _, _, data_log_service, _ = get_services()
    limit = request.args.get('limit', 50, type=int)
    logs = data_log_service.get_recent_logs(limit=limit) if data_log_service else []
    return jsonify(logs)
