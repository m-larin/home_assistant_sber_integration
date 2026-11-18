"""Constants for the Sber Smart Home (SberDevices MQTT) integration."""

DOMAIN = "sber"

# Config entry keys
CONF_DEVICE_ID = "device_id"  # логин интеграции (Device ID) из Сбер Studio
CONF_PASSWORD = "password"  # пароль интеграции
CONF_BROKER = "broker"  # адрес MQTT-брокера Сбера
CONF_PORT = "port"  # порт MQTT-брокера
CONF_HOME = "home"  # имя дома в приложении Салют

# Defaults
DEFAULT_BROKER = "mqtt-partners.iot.sberdevices.ru"
DEFAULT_PORT = 8883
DEFAULT_HOME = "Мой дом"

VERSION = "0.1.0"

# HA domains that are mirrored to Sber
DOMAIN_LIGHT = "light"
DOMAIN_SWITCH = "switch"

# SberDevices MQTT topics
SBER_GLOBAL_TOPIC = "sberdevices/v1/__config"

# The Sber cloud sends its global config (http_api_endpoint) right after a
# device connects and only accepts the device config after that (the working
# SberGate reference explicitly waits for it). If it never arrives we fall
# back to publishing after this delay.
SBER_GLOBAL_CONF_TIMEOUT = 10.0

# Event fired by the entity registry (literal to avoid import surprises)
EVENT_ENTITY_REGISTRY_UPDATED = "entity_registry_updated"
