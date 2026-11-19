# =============================================================================
# ENME 441 - Lab 10: Brokered communication
#
# Team: Haroon Amanullah, Michael Hannah, Ayden Leith, Daniel Gregory
#
# Code template
#
# Two rules:
#   - _on_message() runs inside check_msg().  No handler may await, sleep, or
#     do network I/O.  Set an attribute or create a task.
#   - Nothing here may call time.sleep().
# =============================================================================

import asyncio
import json
import time

from machine import Pin
from umqtt.simple import MQTTClient

import config
import wifi
from shifter import Shifter
from lab8 import Stepper

LASER_PIN = 8           # onboard LED, active LOW -- stands in for the laser

PUBLISH_MS = 1000       # time between publish events

class Turret:
    """Two axes and a laser"""

    def __init__(self, az, alt, laser_pin=LASER_PIN):
        self.az = az
        self.alt = alt
        self.laser = Pin(laser_pin, Pin.OUT)
        self.laser.value(1)             # active LOW: start off

        self.firing = False
        self.message = ''
        self._task = None               # the move or shot in flight
        self.notify = None              # set by Comms; call to push telemetry

    # --- laser ---------------------------------------------------------------

    def laser_on(self):
        self.laser.value(0)

    def laser_off(self):
        self.laser.value(1)

    async def fire(self, seconds):
        """Laser on for exactly `seconds`, then off."""
        self.laser_on()
        self.firing = True
        try:
            await asyncio.sleep(seconds)
        finally:
            self.laser_off()
            self.firing = False

    # --- motion --------------------------------------------------------------

    async def goto(self, az=None, alt=None):
        """Absolute move.  Both axes must arrive together."""
        await Stepper.go_together((self.az, float(az)), (self.alt, float(alt)))
        return

    async def jog(self, axis, deg):
        """Relative move, degrees."""
        if axis == 'az':
            await self.az.rotate(deg)
        elif axis == 'alt':
            await self.alt.rotate(deg)
        
        return

    async def step(self, axis, steps):
        """Relative move, raw half-steps."""
        dir = 1 if steps > 0 else -1

        if axis == 'az':
            await self.az._arm(abs(steps), dir)
        elif axis == 'alt':
            await self.alt._arm(abs(steps), dir)

        return

    async def zero(self, axis=None):
        """Current position becomes 0 deg, for one axis or both."""
        if axis == 'az':
            await self.az.zero()
        elif axis == 'alt':
            await self.alt.zero()
        else:
            await Stepper.go_together((self.az, 0), (self.alt, 0))

        return

    def release(self):
        self.az.release()
        self.alt.release()

    def stop(self):
        """Abort everything.  Laser off first."""
        self.laser_off()
        Stepper.stop()
        self.release()

    # --- state ---------------------------------------------------------------

    def telemetry(self):
        """Return a dict describing the current state."""
        return {
            'az': self.az.angle,
            'alt': self.alt.angle,
            'laser': not self.laser.value(),
            'in_progress': self.az._lock.locked() or self.alt._lock.locked() or self.firing,
            'pending_message': self.message,
            'uptime': time.ticks_ms(),
        }


class Comms:
    """HiveMQ <-> Turret."""

    def __init__(self, turret):
        self.turret = turret
        self.client = None
        self.connected = False

        turret.notify = self.publish_telemetry

        self.dispatch = {
            'GOTO': lambda d: asyncio.create_task(self.turret.goto(d.get('az'), d.get('alt'))),
            'JOG': lambda d: asyncio.create_task(self.turret.jog(d.get('axis'), d.get('deg'))),
            'STEP': lambda d: asyncio.create_task(self.turret.step(d.get('axis'), d.get('steps'))),
            'ZERO': lambda d: asyncio.create_task(self.turret.zero(d.get('axis'))),
            'FIRE': lambda d: asyncio.create_task(self.turret.fire(d.get('seconds'))),
            'STOP': lambda d: self.turret.stop(),
            'LASER_ON': lambda d: self.turret.laser_on(),
            'LASER_OFF': lambda d: self.turret.laser_off(),
            'STATUS': lambda d: self.publish_telemetry(),
        }

    # --- connection ----------------------------------------------------------

    def connect(self):
        """TLS connect, last will, subscribe, announce."""
        self.client = MQTTClient(
            config.TERPMAIL_USERNAME, 
            config.MQTT_BROKER, 
            config.MQTT_PORT,
            config.MQTT_USER, 
            config.MQTT_PASS, 
            keepalive=1,
            ssl=True,
            ssl_params={'server_hostname': config.MQTT_BROKER})
        
        self.client.set_callback(self._on_message)     
        self.client.set_last_will(
            config.TOPIC_STATUS, 
            b'offline', 
            retain=True
        )

        self.client.connect()
        self.client.subscribe(config.TOPIC_COMMANDS)
        self.client.publish(
            config.TOPIC_STATUS, 
            b'online', 
            retain=True
        )

        print(f'Connected to MQTT broker, subscribed to {config.TOPIC_COMMANDS}')

        self.connected = True

    # --- incoming ------------------------------------------------------------

    def _on_message(self, topic, msg):
        """umqtt callback.  Runs inside check_msg(), so keep it short."""
        print(f'Received on {topic}: {msg}')
        try:
            data = json.loads(msg)
            handler = self.dispatch.get(data.get('cmd', ''))

            if handler:
                handler(data)
                self.turret.message = data.get('cmd', '')
                self.publish_telemetry()
            else:
                print(f'Unknown command: {data.get("cmd", "")}')
        except Exception as e:
            print('Error parsing message:', e)

    async def _rx_loop(self):
        """Poll the broker forever."""
        while True:
            self.client.check_msg()
            await asyncio.sleep_ms(100)

    # --- outgoing ------------------------------------------------------------

    def publish_telemetry(self):
        """Send one telemetry packet on the sensor topic."""
        if not self.connected:
            return

        try:
            telemetry = self.turret.telemetry()
            self.client.publish(config.TOPIC_SENSOR, json.dumps(telemetry))
        except Exception as e:
            print('Error publishing telemetry:', e)

    async def _tx_loop(self):
        """Publish telemetry every PUBLISH_MS forever."""
        while True:
            self.publish_telemetry()
            await asyncio.sleep_ms(PUBLISH_MS)

    # --- entry point ---------------------------------------------------------

    def start(self):
        """Launch both loops as tasks.  Returns immediately."""
        self.connect()
        asyncio.create_task(self._rx_loop())
        asyncio.create_task(self._tx_loop())


async def main():
    wifi.connect()

    Stepper.reset()
    s = Shifter(data=5, latch=6, clock=7)
    az = Stepper(s)             # bits 0-3
    alt = Stepper(s)            # bits 4-7
    az.zero()
    alt.zero()

    turret = Turret(az, alt)
    comms = Comms(turret)
    comms.start()

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        Stepper.stop()
        asyncio.new_event_loop()
