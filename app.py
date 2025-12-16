"""
Protocol Bridge - Main Application
Clean architecture with proper initialization order
"""
import os
import logging
from flask import Flask, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from database import db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Database configuration
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///bridge_logic.db")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Initialize database with app
db.init_app(app)

# Initialize services (global references)
ethernetip_service = None
snmp_service = None
mqtt_service = None
data_logging_service = None
polling_service = None


def initialize_app():
    """Initialize application - database, services, routes"""
    global ethernetip_service, snmp_service, mqtt_service, data_logging_service, polling_service
    
    with app.app_context():
        # Import models to register them with SQLAlchemy
        import models
        
        # Create database tables
        db.create_all()
        logger.info("Database tables created")
        
        # Import and register blueprint
        from routes import main_bp
        app.register_blueprint(main_bp)
        logger.info(f"Routes registered: {len(list(app.url_map.iter_rules()))} routes")
        
        # Log all routes for debugging
        for rule in app.url_map.iter_rules():
            logger.info(f"  Route: {rule.rule} -> {rule.endpoint}")
        
        # Initialize services
        try:
            from services import ethernetip_service as eip_svc
            from services import snmp_service as snmp_svc
            from services import mqtt_service as mqtt_svc
            from services import data_logging_service as log_svc
            from services.polling_service import PollingService
            
            ethernetip_service = eip_svc
            snmp_service = snmp_svc
            mqtt_service = mqtt_svc
            data_logging_service = log_svc
            
            # Initialize polling service with all dependencies
            polling_service = PollingService(
                app=app,
                db=db,
                ethernetip_service=ethernetip_service,
                snmp_service=snmp_service,
                mqtt_service=mqtt_service,
                data_logging_service=data_logging_service
            )
            
            logger.info(f"EthernetIP service type: {type(ethernetip_service)}")
            logger.info(f"EthernetIP service: {ethernetip_service}")
            
            # Store services in app config for access from routes
            app.config['ethernetip_service'] = ethernetip_service
            app.config['snmp_service'] = snmp_service
            app.config['mqtt_service'] = mqtt_service
            app.config['data_logging_service'] = data_logging_service
            app.config['polling_service'] = polling_service
            
            logger.info("Services initialized and stored in app config successfully")
            # logger.info(f"Services in config: {list(app.config.keys())}")
            
            # Auto-connect all enabled devices on startup
            logger.info("=== Auto-connecting devices on startup ===")
            
            # Connect EthernetIP devices
            eip_configs = db.session.query(models.EthernetIPConfig).filter_by(enabled=True).all()
            for config in eip_configs:
                success, message = ethernetip_service.connect_device(config)
                if success:
                    logger.info(f"✓ Connected to EthernetIP device: {config.name}")
                else:
                    logger.warning(f"✗ Failed to connect to {config.name}: {message}")
            
            # Connect SNMP devices
            snmp_configs = db.session.query(models.SNMPConfig).filter_by(enabled=True).all()
            for config in snmp_configs:
                success, message = snmp_service.connect_device(config)
                if success:
                    logger.info(f"✓ Connected to SNMP device: {config.name}")
                else:
                    logger.warning(f"✗ Failed to connect to {config.name}: {message}")
            
            # Connect MQTT brokers and start subscribers
            mqtt_configs = db.session.query(models.MQTTConfig).filter_by(enabled=True).all()
            for config in mqtt_configs:
                success, message = mqtt_service.connect_broker(config)
                if success:
                    logger.info(f"✓ Connected to MQTT broker: {config.name}")
                    # Start subscriber for two-way communication if configured
                    if config.subscribe_topic:
                        sub_success, sub_message = mqtt_service.start_subscriber(config, app)
                        if sub_success:
                            logger.info(f"✓ Started MQTT subscriber for {config.name} on topic: {config.subscribe_topic}")
                        else:
                            logger.warning(f"✗ Failed to start subscriber for {config.name}: {sub_message}")
                else:
                    logger.warning(f"✗ Failed to connect to {config.name}: {message}")
            
            # Start polling service
            polling_service.start()
            logger.info("=== Polling Service Started - Data collection active ===")
            print("🚀 Polling Service Started - Automatic data collection and MQTT publishing active!")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    db.session.rollback()
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == "__main__":
    # Prevent double initialization in Flask debug mode
    import sys
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or '--no-reload' in sys.argv:
        logger.info("Starting Protocol Bridge Application")
        initialize_app()
        logger.info("Flask web server starting on http://127.0.0.1:5000")
    else:
        # First run - only initialize database
        with app.app_context():
            import models
            db.create_all()
        logger.info("Initial run - waiting for reloader...")
    
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        use_reloader=True
    )
