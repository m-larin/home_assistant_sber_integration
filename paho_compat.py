"""Compatibility helpers for paho-mqtt 1.x and 2.x.

Home Assistant containers often ship paho-mqtt 1.6.1 as a dependency of
the built-in MQTT integration, while the integration may also be used
with paho 2.x. paho 2.x requires an explicit ``callback_api_version``
argument, paho 1.x does not accept it at all, so we create the client
conditionally.
"""

from __future__ import annotations

import paho.mqtt.client as mqtt

try:
    from paho.mqtt.enums import CallbackAPIVersion

    HAS_PAHO_V2 = True
except ImportError:  # paho-mqtt 1.x
    CallbackAPIVersion = None  # type: ignore[assignment]
    HAS_PAHO_V2 = False


def new_client(client_id: str) -> mqtt.Client:
    """Create a paho MQTT client compatible with paho 1.x and 2.x.

    Uses the v1 callback API (integer return codes) on both versions.
    """
    if HAS_PAHO_V2:
        return mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    return mqtt.Client(client_id=client_id)
