"""Config flow for the Sber Smart Home (SberDevices MQTT) integration."""

from __future__ import annotations

import asyncio
import logging
import queue
import ssl

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_PASSWORD, CONF_PORT
from homeassistant.core import callback

from .const import (
    CONF_BROKER,
    CONF_DEVICE_ID,
    CONF_HOME,
    DEFAULT_BROKER,
    DEFAULT_HOME,
    DEFAULT_PORT,
    DOMAIN,
)
from .paho_compat import new_client

_LOGGER = logging.getLogger(__name__)


async def async_test_sber_connection(
    device_id: str,
    password: str,
    broker: str,
    port: int,
) -> tuple[bool, str | None]:
    """Try to reach the Sber broker over TLS and attempt an MQTT connect.

    Returns a tuple ``(reachable, error)``. ``reachable`` is True when a
    TCP/TLS connection to the broker was established (regardless of the
    MQTT auth outcome), so that a wrong password does not block setup.
    """
    result: queue.Queue[tuple[bool, str | None]] = queue.Queue(maxsize=1)

    client = new_client(client_id="ha-sber-test")
    client._test_result = result  # type: ignore[attr-defined]

    def _on_connect(_c, _ud, _flags, rc, *props):  # noqa: ARG001
        client._test_result.put_nowait((True, None))  # type: ignore[union-attr]

    def _on_disconnect(_c, _ud, rc, *props):  # noqa: ARG001
        # paho 2.x passes a reason-code object; normalize to int
        rc_value = getattr(rc, "value", rc)
        if rc_value != 0:
            try:
                client._test_result.put_nowait(  # type: ignore[union-attr]
                    (False, f"rc={rc_value}")
                )
            except queue.Full:
                pass

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.username_pw_set(device_id, password)
    # Sber cloud broker uses TLS; we do not verify the certificate here
    # (same approach as the reference SberGate add-on).
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

    try:
        client.connect_async(broker, port, 60)
        client.loop_start()
        try:
            return await asyncio.get_running_loop().run_in_executor(
                None, result.get
            )
        except asyncio.TimeoutError:
            return False, "timeout"
    except OSError as err:
        return False, str(err)
    finally:
        try:
            client.loop_stop()  # paho 1.6 has no timeout arg
            client.disconnect()
        except (ValueError, OSError):
            pass


DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DEVICE_ID): vol.Coerce(str),
        vol.Required(CONF_PASSWORD): vol.Coerce(str),
        vol.Required(CONF_BROKER, default=DEFAULT_BROKER): vol.Coerce(str),
        vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=65535)
        ),
        vol.Optional(CONF_HOME, default=DEFAULT_HOME): vol.Coerce(str),
    }
)


class SberConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Sber Smart Home."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            connected, error = await async_test_sber_connection(
                user_input[CONF_DEVICE_ID],
                user_input[CONF_PASSWORD],
                user_input[CONF_BROKER],
                user_input[CONF_PORT],
            )
            _LOGGER.debug("Sber broker %s:%s reachable=%s (%s)",
                          user_input[CONF_BROKER], user_input[CONF_PORT],
                          connected, error)
            if not connected:
                errors["base"] = "cannot_connect"
            else:
                # Prevent duplicate entries for the same broker + device id
                await self.async_set_unique_id(
                    f"{user_input[CONF_BROKER]}:{user_input[CONF_PORT]}:"
                    f"{user_input[CONF_DEVICE_ID]}"
                )
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Sber ({user_input[CONF_DEVICE_ID]})",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user", data_schema=DATA_SCHEMA, errors=errors
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> SberOptionsFlow:
        """Get the options flow for this entry."""
        return SberOptionsFlow()


class SberOptionsFlow(OptionsFlow):
    """Handle options for the Sber Smart Home integration."""

    async def async_step_init(
        self, user_input: dict | None = None
    ) -> ConfigFlowResult:
        """Manage the options (house name)."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        # The saved value lives in the entry options; fall back to the
        # config entry data and finally the default.
        current = self.config_entry.options.get(
            CONF_HOME,
            self.config_entry.data.get(CONF_HOME, DEFAULT_HOME),
        )
        options_schema = vol.Schema(
            {
                vol.Optional(CONF_HOME, default=current): vol.Coerce(str),
            }
        )
        return self.async_show_form(step_id="init", data_schema=options_schema)
