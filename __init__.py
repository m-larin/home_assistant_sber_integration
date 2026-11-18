"""The Sber Smart Home (SberDevices MQTT) integration.

Bridges Home Assistant ``light`` and ``switch`` entities with the Sber
Smart Home (Salute) cloud over the SberDevices MQTT protocol.

* existing HA lights/switches are advertised to the Sber cloud as
  ``light`` / ``relay`` devices, so they can be controlled from the
  Salute app and by the Salute smart speaker;
* commands received from Sber are translated into HA service calls
  (``light.turn_on/off``, ``switch.turn_on/off``);
* HA state changes are pushed to the Sber cloud so the Salute app and
  the speaker always reflect the real device state.

Only the SberDevices MQTT channel is used. No Sber or HA HTTP/REST or
WebSocket API is involved.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import ssl
import uuid
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    CONF_BROKER,
    CONF_DEVICE_ID,
    CONF_ENTITIES,
    CONF_HOME,
    CONF_PASSWORD,
    CONF_PORT,
    DEFAULT_HOME,
    DOMAIN,
    EVENT_ENTITY_REGISTRY_UPDATED,
    SBER_GLOBAL_CONF_TIMEOUT,
    SBER_GLOBAL_TOPIC,
    VERSION,
)

_LOGGER = logging.getLogger(__name__)

# Sber feature value types
T_BOOL = "BOOL"
T_INTEGER = "INTEGER"
T_COLOUR = "COLOUR"

# Approximate mapping of Sber's 0..1000 warm->cold scale to Kelvin
SBER_CT_MIN_K = 2700
SBER_CT_MAX_K = 6500
SBER_CT_SCALE = 1000.0

# Sber minimum brightness (50..1000); keep 1% (10 units) as the floor
SBER_MIN_BRIGHTNESS = 10


# ---------------------------------------------------------------------------
# Sber model / feature helpers
# ---------------------------------------------------------------------------
def _device_descriptor(entity_id: str, name: str, features: list[str]) -> dict:
    """Build one entry for the Sber ``up/config`` device list.

    The shape mirrors the working SberGate reference: no ``parent_id``
    (the cloud attaches devices to the session's hub itself) and no
    ``groups``.
    """
    category = "light" if entity_id.startswith("light.") else "relay"
    return {
        "id": entity_id,
        "name": name,
        "default_name": name,
        "home": "",  # filled in by the gateway with the configured house
        "room": "",
        "hw_version": "1.0",
        "sw_version": VERSION,
        "model": {
            "id": f"ha_{category}",
            "manufacturer": "Home Assistant",
            "model": f"HomeAssistant {category}",
            "category": category,
            "features": features,
        },
        "model_id": "",
    }


def _kelvin_to_sber(kelvin: float) -> int:
    """Map Kelvin to Sber's 0..1000 warm->cold scale."""
    pct = (kelvin - SBER_CT_MIN_K) / (SBER_CT_MAX_K - SBER_CT_MIN_K)
    return int(round(max(0.0, min(1.0, pct)) * SBER_CT_SCALE))


def _sber_to_kelvin(value: int) -> float:
    """Map Sber's 0..1000 scale back to Kelvin."""
    pct = max(0.0, min(1.0, value / SBER_CT_SCALE))
    return SBER_CT_MIN_K + pct * (SBER_CT_MAX_K - SBER_CT_MIN_K)


def _hsv_to_rgb(h: float, s: float, v: float) -> tuple[int, int, int]:
    """Convert HSV (h: 0..360, s/v: 0..1) to 8-bit RGB."""
    import colorsys

    r, g, b = colorsys.hsv_to_rgb(h % 360.0 / 360.0, s, v)
    return int(round(r * 255)), int(round(g * 255)), int(round(b * 255))


def _light_features(attributes: dict[str, Any]) -> list[str]:
    """Return Sber feature keys supported by a HA light entity.

    The advertised capabilities must reflect what the device *can* do (even
    while it is off) so Sber can send the corresponding commands. When
    ``supported_color_modes`` is available it is the source of truth;
    otherwise we fall back to inferring capabilities from attributes.
    """
    features = ["online", "on_off"]
    modes = attributes.get("supported_color_modes")
    if modes is not None:
        mode_strs = {str(getattr(m, "value", m)) for m in modes}
        has_color_temp = "color_temp" in mode_strs
        has_color = bool(mode_strs & {"hs", "xy", "rgb", "rgbw", "rgbww"})
        has_brightness = bool(mode_strs - {"onoff", "unknown"}) or has_color or has_color_temp
    else:
        has_color_temp = (
            attributes.get("color_temp") is not None
            or "min_mireds" in attributes
            or "max_mireds" in attributes
        )
        has_color = any(k in attributes for k in ("hs_color", "rgb_color", "xy_color"))
        has_brightness = (
            attributes.get("brightness") is not None
            or has_color
            or has_color_temp
        )
    if has_brightness:
        features.append("light_brightness")
    if has_color_temp:
        features.append("light_colour_temp")
    if has_color:
        features.append("light_colour")
    return features


def _ha_to_sber_states(
    domain: str, state: str, attributes: dict[str, Any]
) -> tuple[dict[str, list], list[str]] | None:
    """Map an HA light/switch state to (``{"states": [...]}``, features).

    The returned feature list reflects the entity's capabilities so the
    advertised device model stays accurate. Returns ``None`` when nothing
    meaningful can be advertised.
    """
    states: list[dict] = [
        {"key": "online", "value": {"type": T_BOOL, "bool_value": True}},
        {"key": "on_off", "value": {"type": T_BOOL, "bool_value": state == "on"}},
    ]
    if domain == "light":
        features = _light_features(attributes)
    else:
        features = ["online", "on_off"]

    if domain == "light" and state == "on":
        color_temp = attributes.get("color_temp")
        if color_temp is not None:
            # HA color_temp is in mired; Sber wants 0..1000 warm->cold.
            kelvin = 1_000_000 / max(1, color_temp)
            states.append(
                {
                    "key": "light_colour_temp",
                    "value": {
                        "type": T_INTEGER,
                        "integer_value": _kelvin_to_sber(kelvin),
                    },
                }
            )
        elif "rgb_color" in attributes:
            r, g, b = attributes["rgb_color"]
            if not (r == g == b == 255):
                import colorsys

                h, s, v = colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)
                states.append(
                    {
                        "key": "light_colour",
                        "value": {
                            "type": T_COLOUR,
                            "colour_value": {
                                "h": int(round(h * 360)) % 360,
                                "s": int(round(s * 1000)),
                                "v": max(100, int(round(v * 1000))),
                            },
                        },
                    }
                )
        brightness = attributes.get("brightness")
        if brightness is not None:
            states.append(
                {
                    "key": "light_brightness",
                    "value": {
                        "type": T_INTEGER,
                        "integer_value": max(
                            SBER_MIN_BRIGHTNESS,
                            int(round(brightness / 255 * 1000)),
                        ),
                    },
                }
            )

    return {"states": states}, features


# ---------------------------------------------------------------------------
# MQTT gateway
# ---------------------------------------------------------------------------
class SberGateway:
    """Owns the paho MQTT client and the mirrored-device cache."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        # entity_id -> {"name", "features", "states"} (states = published dict)
        self.devices: dict[str, dict[str, Any]] = {}
        self._unsub: list[Any] = []

        merged = {**entry.data, **(entry.options or {})}
        self.device_id: str = merged[CONF_DEVICE_ID]
        self.password: str = merged[CONF_PASSWORD]
        self.broker: str = merged[CONF_BROKER]
        self.port: int = int(merged[CONF_PORT])
        self.home: str = merged.get(CONF_HOME, DEFAULT_HOME)
        # Optional whitelist of HA entity_ids to expose to Sber. When empty /
        # unset (None) we keep the original behaviour and mirror everything.
        raw_entities = merged.get(CONF_ENTITIES)
        self.entity_filter: list[str] | None = (
            [str(e) for e in raw_entities] if raw_entities else None
        )

        self.root_topic = f"sberdevices/v1/{self.device_id}"
        self.down_topic = f"{self.root_topic}/down"

        self._client: mqtt.Client | None = None
        self._connected_event = asyncio.Event()
        self._ever_connected = False
        self._connect_error: str | None = None
        # Track connection state ourselves: paho exposes no public
        # ``Client.connected`` attribute, so we cannot read it.
        self._connected = False
        # The Sber cloud sends a global config (``sberdevices/v1/__config``)
        # right after a device connects and only accepts the device config
        # afterwards (the working SberGate reference waits for it too).
        # Until it arrives (or the fallback timeout elapses) the first
        # ``up/config`` publish is deferred.
        self._global_config_event = asyncio.Event()
        self._config_published_once = False
        self._first_publish_task: asyncio.Task | None = None

    # -- client lifecycle ---------------------------------------------------
    def _start_client(self) -> None:
        """Create the paho client and start its network thread (blocking IO).

        Sber requires TLS on port 8883 and does not present a
        verifiable certificate chain, so we mirror the reference SberGate
        add-on: TLS without certificate verification.
        """
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=f"sber-ha-{uuid.uuid4().hex[:12]}",
        )
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_connect_fail = self._on_connect_fail
        client.on_message = self._on_message
        client.username_pw_set(self.device_id, self.password)
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)

        self._client = client
        client.connect_async(self.broker, self.port, 60)
        client.loop_start()

    def stop(self) -> None:
        """Stop the MQTT client (always run in an executor)."""
        client = self._client
        self._client = None
        self._connected = False
        if client is not None:
            with contextlib.suppress(Exception):
                # Stop the network loop, then drop the socket.
                client.loop_stop()
                client.disconnect()

    # -- paho callbacks (run on the paho network thread) --------------------
    def _on_connect(self, _client, _userdata, _flags, rc, *props):
        # paho 2.x (VERSION1) passes a reason-code object; normalize to int
        rc_value = getattr(rc, "value", rc)
        self.hass.loop.call_soon_threadsafe(self._async_on_connect, rc_value)

    def _on_disconnect(self, _client, _userdata, rc, *props):
        # paho 2.x (VERSION2) passes a reason-code object; normalize to int
        rc_value = getattr(rc, "value", rc)
        if rc_value != 0:
            _LOGGER.warning("Sber MQTT unexpected disconnect, rc=%s", rc_value)
        self.hass.loop.call_soon_threadsafe(self._async_on_disconnect)

    def _on_connect_fail(self, _client):
        """Called once when the initial connection is refused (v1 callback API)."""
        self.hass.loop.call_soon_threadsafe(self._async_on_connect_fail)

    def _on_message(self, _client, _userdata, msg: mqtt.MQTTMessage) -> None:
        if msg.topic == f"{self.down_topic}/commands":
            # hass.create_task is the modern API; fall back for older HA
            create_task = getattr(self.hass, "create_task", None)
            if create_task is None:
                self.hass.async_create_task(self._handle_commands(msg.payload))
            else:
                create_task(self._handle_commands(msg.payload))
        elif msg.topic == f"{self.down_topic}/status_request":
            self.hass.loop.call_soon_threadsafe(
                self._handle_status_request, msg.payload
            )
        elif msg.topic == f"{self.down_topic}/config_request":
            self.hass.loop.call_soon_threadsafe(self._publish_config)
        elif msg.topic == f"{self.down_topic}/errors":
            _LOGGER.warning("Sber error: %s", msg.payload)
        elif msg.topic == SBER_GLOBAL_TOPIC:
            _LOGGER.debug("Sber global config: %s", msg.payload)
            self.hass.loop.call_soon_threadsafe(self._async_on_global_config)

    # -- event-loop side ----------------------------------------------------
    @callback
    def _async_on_connect(self, rc: int) -> None:
        # paho 2.x (VERSION2) would pass a ReasonCode; normalize to int
        rc = int(getattr(rc, "value", rc))
        if rc != 0:
            _LOGGER.error(
                "Sber MQTT connect failed, rc=%s (%s)",
                rc,
                mqtt.connack_string(rc),
            )
            self._connect_error = f"connect failed, rc={rc}"
            if not self._connected_event.is_set():
                self._connected_event.set()  # release the wait; state checked after
            return
        _LOGGER.info("Sber MQTT connected to %s:%s", self.broker, self.port)
        self._ever_connected = True
        self._connected = True
        try:
            # paho requires a list of (topic, qos) tuples
            # when subscribing to several topics at once.
            self._client.subscribe(
                [
                    (f"{self.down_topic}/commands", 0),
                    (f"{self.down_topic}/status_request", 0),
                    (f"{self.down_topic}/config_request", 0),
                    (f"{self.down_topic}/errors", 0),
                    (SBER_GLOBAL_TOPIC, 0),
                ]
            )
        except Exception as err:  # noqa: BLE001
            self._connect_error = f"subscribe failed: {err}"
            _LOGGER.error("Sber MQTT subscribe failed: %s", err)
            if not self._connected_event.is_set():
                self._connected_event.set()
            return
        self._connected_event.set()
        self._track_entities()
        self._publish_config()

    @callback
    def _async_on_disconnect(self) -> None:
        self._connected = False
        if self._ever_connected:
            self._connected_event.clear()

    @callback
    def _async_on_connect_fail(self) -> None:
        _LOGGER.error("Sber MQTT initial connection failed")
        if not self._connected_event.is_set():
            self._connected_event.set()  # release the wait; state checked after

    @callback
    def _async_on_global_config(self) -> None:
        """The broker sent its global config: it now accepts device configs."""
        _LOGGER.info("Sber global config received")
        self._global_config_event.set()

    def _publish(self, topic: str, payload: str) -> None:
        client = self._client
        if client is None:
            return
        try:
            client.publish(topic, payload, qos=0)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Sber MQTT publish to %s failed: %s", topic, err)

    @callback
    def _publish_config(self) -> None:
        """Publish the full device list to Sber.

        The first publish is deferred until the broker sends its global
        config (see :meth:`_async_first_publish`) — the Sber cloud only
        accepts a device config for a session after that, exactly like the
        reference SberGate waits. Later publishes are immediate.
        """
        if self._config_published_once:
            self._do_publish_config()
            return
        if self._first_publish_task is None or self._first_publish_task.done():
            create_task = getattr(self.hass, "create_task", None)
            if create_task is None:
                self._first_publish_task = self.hass.async_create_task(
                    self._async_first_publish()
                )
            else:
                self._first_publish_task = create_task(
                    self._async_first_publish()
                )

    async def _async_first_publish(self) -> None:
        """Wait for the Sber global config, then publish the device list."""
        try:
            await asyncio.wait_for(
                self._global_config_event.wait(),
                timeout=SBER_GLOBAL_CONF_TIMEOUT,
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Sber global config not received within %.0f s; "
                "publishing device config anyway",
                SBER_GLOBAL_CONF_TIMEOUT,
            )
        if not self._connected:
            return
        self._config_published_once = True
        self._do_publish_config()

    @callback
    def _do_publish_config(self) -> None:
        """Build and publish the device list payload.

        The Sber cloud expects a root hub device first in the list; the HA
        entities follow as siblings (same shape as the reference SberGate).
        """
        devices = [
            {
                "id": "root",
                "name": "HA Gateway",
                "hw_version": "1.0",
                "sw_version": VERSION,
                "model": {
                    "id": "ha_root_hub",
                    "manufacturer": "Home Assistant",
                    "model": "HA Gateway Hub",
                    "description": "Home Assistant Sber Gateway",
                    "category": "hub",
                    "features": ["online"],
                },
            }
        ]
        for entity_id, dev in self.devices.items():
            desc = _device_descriptor(entity_id, dev["name"], dev["features"])
            desc["home"] = self.home
            devices.append(desc)
        self._publish(
            f"{self.root_topic}/up/config",
            json.dumps({"devices": devices}, ensure_ascii=False),
        )
        _LOGGER.info("Sber up/config published (%d devices)", len(devices))

    @callback
    def _publish_states(self, entity_ids: list[str]) -> None:
        """Publish current states for the given entities to Sber.

        States for devices the cloud does not know yet are meaningless,
        so nothing is sent until the first device config went out.
        """
        if not self._config_published_once:
            return
        devices: dict[str, dict] = {}
        for entity_id in entity_ids:
            dev = self.devices.get(entity_id)
            if dev and dev.get("states"):
                devices[entity_id] = dev["states"]
        if not devices:
            return
        self._publish(
            f"{self.root_topic}/up/status",
            json.dumps({"devices": devices}, ensure_ascii=False),
        )

    # -- Sber command handling ----------------------------------------------
    async def _handle_commands(self, payload: bytes | str) -> None:
        try:
            data = json.loads(payload)
        except (ValueError, TypeError) as err:
            _LOGGER.warning("Sber bad commands payload: %s", err)
            return
        for entity_id, dev in (data.get("devices") or {}).items():
            known = self.devices.get(entity_id)
            if not known:
                _LOGGER.debug("Sber command for unknown device %s", entity_id)
                continue
            for item in dev.get("states") or []:
                key = item.get("key")
                if key not in known["features"]:
                    continue
                raw = item.get("value") or {}
                try:
                    await self._apply_command(entity_id, key, raw)
                except Exception:  # noqa: BLE001
                    _LOGGER.exception(
                        "Sber command failed for %s (%s)", entity_id, key
                    )

    async def _apply_command(self, entity_id: str, key: str, raw: dict) -> None:
        domain, _ = entity_id.split(".", 1)
        if key == "on_off":
            service = "turn_on" if raw.get("bool_value") else "turn_off"
            await self._call_service(entity_id, domain, service)
        elif key == "light_brightness":
            value = raw.get("integer_value")
            if value is None:
                return
            await self._call_service(
                entity_id, domain, "turn_on",
                brightness=round(int(value) / 10),
            )
        elif key == "light_colour_temp":
            value = raw.get("integer_value")
            if value is None:
                return
            await self._call_service(
                entity_id, domain, "turn_on",
                kelvin=round(_sber_to_kelvin(int(value))),
            )
        elif key == "light_colour":
            colour = raw.get("colour_value")
            if not colour:
                return
            r, g, b = _hsv_to_rgb(
                colour.get("h", 0),
                colour.get("s", 0) / 1000.0,
                max(0.1, colour.get("v", 100) / 1000.0),
            )
            await self._call_service(
                entity_id, domain, "turn_on",
                rgb_color=[r, g, b],
                brightness=round(colour.get("v", 100)),
            )

    async def _call_service(self, entity_id: str, domain: str, service: str, **kwargs) -> None:
        data: dict[str, Any] = {"entity_id": entity_id}
        data.update(kwargs)
        _LOGGER.debug("Sber -> HA %s.%s %s", domain, service, data)
        await self.hass.services.async_call(domain, service, data, blocking=True)

    # -- HA entity tracking ---------------------------------------------------
    def _entity_allowed(self, entity_id: str) -> bool:
        """Return True when *entity_id* should be exposed to Sber.

        When the user configured a device whitelist (options → devices) only
        those entities are published; otherwise every light/switch is used.
        """
        if not self.entity_filter:
            return True
        return entity_id in self.entity_filter

    @callback
    def _track_entities(self) -> None:
        """(Re)build the mirrored device set from HA's current states."""
        states = self.hass.states.async_all(["light", "switch"])
        seen: set[str] = set()
        for state in states:
            if not self._entity_allowed(state.entity_id):
                continue
            seen.add(state.entity_id)
            self._register_entity(state.entity_id, state)

        for entity_id in [e for e in self.devices if e not in seen]:
            del self.devices[entity_id]
            _LOGGER.info("Sber device removed: %s", entity_id)

        if self._client is not None and self._connected:
            self._publish_config()

    @callback
    def _register_entity(self, entity_id: str, state) -> None:
        dev = self.devices.get(entity_id)
        if dev is None:
            if state.domain == "light":
                features = _light_features(state.attributes)
            else:
                features = ["online", "on_off"]
            dev = {
                "name": state.attributes.get(
                    "friendly_name", entity_id.split(".", 1)[1].replace("_", " ").title()
                ),
                "features": features,
                "states": None,
            }
            self.devices[entity_id] = dev
            _LOGGER.info("Sber device added: %s", entity_id)
            self._publish_config()
            self._unsub.append(
                async_track_state_change_event(
                    self.hass,
                    [entity_id],
                    lambda event: self._async_on_state_changed(event),
                )
            )
        else:
            name = state.attributes.get("friendly_name")
            if name:
                dev["name"] = name

        self._sync_state(entity_id)

    def _sync_state(self, entity_id: str) -> None:
        """Update the cached Sber state from HA and publish to Sber.

        Advertised features follow the entity's capabilities (not the
        transient state), so a light keeps its colour/brightness features
        even while off and Sber can still send those commands.
        """
        dev = self.devices.get(entity_id)
        if dev is None:
            return
        state = self.hass.states.get(entity_id)
        if state is None:
            return
        mapped = _ha_to_sber_states(state.domain, state.state, state.attributes)
        if mapped is None:
            return
        new_states, new_features = mapped
        changed = False
        if new_features != dev.get("features"):
            dev["features"] = new_features
            changed = True
        if dev.get("states") != new_states:
            dev["states"] = new_states
            self._publish_states([entity_id])
            # states are part of the device snapshot; config republish is
            # only needed when the advertised capability set changes.
        if changed and self._connected:
            self._publish_config()

    @callback
    def _async_on_state_changed(self, event) -> None:
        self._sync_state(event.data["entity_id"])

    @callback
    def _handle_status_request(self, payload: bytes | str) -> None:
        """Answer Sber's status_request with current device states."""
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            data = {}
        requested = data.get("devices")
        if isinstance(requested, list):
            entity_ids = [e for e in requested if e in self.devices]
        else:
            entity_ids = list(self.devices)
        self._publish_states(entity_ids)

    # -- setup / teardown -----------------------------------------------------
    async def async_setup(self) -> None:
        """Start the gateway. Raises ConfigEntryNotReady on failure."""
        self._unsub.append(
            self.hass.bus.async_listen(
                EVENT_ENTITY_REGISTRY_UPDATED,
                self._on_registry_updated,
            )
        )
        self._unsub.append(
            self.hass.bus.async_listen(
                EVENT_HOMEASSISTANT_STOP,
                lambda _e: self.hass.async_add_executor_job(self.stop),
            )
        )
        await self.hass.async_add_executor_job(self._start_client)
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            await self.hass.async_add_executor_job(self.stop)
            raise ConfigEntryNotReady(
                f"Cannot connect to Sber MQTT broker {self.broker}:{self.port}"
            )
        if self._connect_error:
            await self.hass.async_add_executor_job(self.stop)
            raise ConfigEntryNotReady(
                f"Cannot connect to Sber MQTT broker {self.broker}:{self.port}: "
                f"{self._connect_error}"
            )
        if self._client is None or not self._connected:
            await self.hass.async_add_executor_job(self.stop)
            raise ConfigEntryNotReady(
                f"Cannot connect to Sber MQTT broker {self.broker}:{self.port}"
            )

    @callback
    def _on_registry_updated(self, _event) -> None:
        """Re-track entities when the entity registry changes."""
        self._track_entities()

    async def async_unload(self) -> None:
        """Stop the gateway and remove listeners."""
        for unsub in self._unsub:
            unsub()
        self._unsub.clear()
        if self._first_publish_task is not None and not self._first_publish_task.done():
            self._first_publish_task.cancel()
            self._first_publish_task = None
        await self.hass.async_add_executor_job(self.stop)


# ---------------------------------------------------------------------------
# Config entry setup
# ---------------------------------------------------------------------------
async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Sber Smart Home from a config entry."""
    gateway = SberGateway(hass, entry)
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = gateway

    try:
        await gateway.async_setup()
    except ConfigEntryNotReady:
        hass.data[DOMAIN].pop(entry.entry_id, None)
        raise

    try:
        # HA 2024.6+
        entry.async_on_unload(entry.add_update_listener(_async_reload))
    except AttributeError:
        # older HA: no update listener; manual reload works as before
        pass
    return True


async def _async_reload(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry when its configuration changes."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    gateway = hass.data[DOMAIN].get(entry.entry_id)
    if gateway:
        await gateway.async_unload()
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return True


@callback
def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate an existing config entry to the latest version."""
    if entry.version == 1:
        return True
    _LOGGER.warning("Sber migration from version %s not supported", entry.version)
    return False
