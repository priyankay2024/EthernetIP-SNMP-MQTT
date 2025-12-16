# MQTT Topic Configuration Simplification

## Summary of Changes

This update simplifies the MQTT broker configuration by removing the confusing "Topic Prefix" field and making "Publish Topic" the primary topic configuration field.

## Changes Made

### 1. Database Model (`models.py`)
- **Removed**: `topic_prefix` field from `MQTTConfig` model
- **Updated**: `publish_topic` field is now required (`nullable=False`)

### 2. HTML Template (`templates/config_mqtt.html`)
- **Removed**: "Topic Prefix" field from both Add and Edit forms
- **Removed**: "Topic Prefix" column from the broker listing table
- **Updated**: "Publish Topic (optional)" label changed to "Publish Topic" and made required
- **Removed**: JavaScript code that handled `editTopicPrefix` field

### 3. Routes (`routes.py`)
- **Removed**: All references to `topic_prefix` in add, update, and API operations
- **Added**: Validation to ensure `publish_topic` is provided for both add and update operations
- **Updated**: Error handling with flash messages for missing publish topic

### 4. Polling Service (`services/polling_service.py`)
- **Updated**: Topic construction now uses `mqtt_config.publish_topic` instead of `topic_prefix`
- **Format**: Topics are now constructed as `{publish_topic}/{device_identifier}`
- **Added**: Check to skip publishing if no publish_topic is configured

### 5. Database Migration (`migrate_topic_prefix.py`)
- **Created**: Migration script to handle existing data
- **Function**: Copies `topic_prefix` values to `publish_topic` for existing records where needed
- **Result**: Seamless transition from old to new schema

## How It Works Now

### Before (Confusing):
- **Topic Prefix**: Used to construct topics like `bridge/device123`
- **Publish Topic**: Used for specific purposes (optional)
- **Result**: Two similar fields causing confusion

### After (Simplified):
- **Publish Topic**: Single field for all data publishing
- **Format**: `{publish_topic}/{device_identifier}`
- **Example**: If publish_topic is "sensors/data", final topic becomes "sensors/data/device123"
- **Required**: All MQTT configurations must have a publish topic

## Migration Process

1. **Automatic**: Run `python migrate_topic_prefix.py` (already completed)
2. **Data Safety**: Existing `topic_prefix` values were copied to `publish_topic` where needed
3. **Backwards Compatible**: No data loss during migration

## Benefits

✅ **Simplified Configuration**: Single topic field eliminates confusion
✅ **Clear Purpose**: Publish Topic is the primary field for data publishing  
✅ **Required Field**: Ensures all MQTT configurations have proper topics
✅ **Consistent Behavior**: All data publishing uses the same topic logic
✅ **Better UX**: Users know exactly which field to configure

## Usage

When configuring MQTT brokers, users now only need to specify:
- **Publish Topic**: The base topic for publishing data (e.g., "sensors/data", "factory/line1")
- Data will be published to: `{publish_topic}/{device_hwid_or_id}`

Example:
- Publish Topic: "factory/sensors"
- Device HWID: "PLC001"  
- Final Topic: "factory/sensors/PLC001"