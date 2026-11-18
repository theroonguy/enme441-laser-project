# =============================================================================
# ENME 441 - Lab 10: Brokered communication
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

PUBLISH_MS = 2000       # time between publish events

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
        self.laser.value(1)

    def laser_off(self):
        self.laser.value(0)

    async def fire(self, seconds=None):
        """Laser on for exactly `seconds`, then off."""
        self.laser.value(1)
        try:
            await asyncio.sleep(seconds)
        finally:
            self.laser.value(0)

    # --- motion --------------------------------------------------------------

    def goto(self, az=None, alt=None):
        """Absolute move.  Both axes must arrive together."""
        Stepper.move_together((self.az, az), (self.alt, alt))

    def jog(self, axis, deg):
        """Relative move, degrees."""
        if axis == 'az':
            self.az.rotate(deg)
        elif axis == 'alt':
            self.alt.rotate(deg)

    def step(self, axis, steps):
        """Relative move, raw half-steps."""
        dir = 1 if steps > 0 else -1

        if axis == 'az':
            self.az._arm(abs(steps), dir)
        elif axis == 'alt':
            self.alt._arm(abs(steps), dir)

    def zero(self, axis=None):
        """Current position becomes 0 deg, for one axis or both."""
        if axis == 'az':
            self.az.zero()
        elif axis == 'alt':
            self.alt.zero()
        else:
            self.az.zero()
            self.alt.zero()

    def release(self):
        self.az.release()
        self.alt.release()

    def stop(self):
        """Abort everything.  Laser off first."""
        self.laser_off()
        Stepper.stop()
        self.az.release()
        self.alt.release()

    # --- state ---------------------------------------------------------------

    def telemetry(self):
        """Return a dict describing the current state."""
        return {
            'az': self.az.angle(),
            'alt': self.alt.angle(),
            'laser': self.laser.value(),
            'in_progress': self.az._lock.locked() or self.alt._lock.locked(),
            'pending_message': self.message,
        }


class Comms:
    """HiveMQ <-> Turret."""

    def __init__(self, turret):
        self.turret = turret
        self.client = None
        self.connected = False

        turret.notify = self.publish_telemetry

        self.dispatch = {
            # 'CMD': lambda d: ...,
            # 'LED_ON': 
        }

    # --- connection ----------------------------------------------------------

    def connect(self):
        """TLS connect, last will, subscribe, announce."""
        self.client = MQTTClient(config.TERPMAIL_USERNAME, config.MQTT_BROKER, config.MQTT_PORT,
                        config.MQTT_USER, config.MQTT_PASS, ssl=True,
                        ssl_params={'server_hostname': config.MQTT_BROKER})
        self.client.set_callback(self._on_message)     # set callback for check_msg()
        self.client.connect()
        self.client.subscribe(config.TOPIC_COMMANDS)
        self.client.set_last_will(config.TOPIC_STATUS, b'offline', retain=True)
        self.client.publish(config.TOPIC_STATUS, b'online', retain=True)

        print(f'Connected to MQTT broker, subscribed to {config.TOPIC_COMMANDS}')

        self.connected = True

    # --- incoming ------------------------------------------------------------

    def _on_message(self, topic, msg):
        """umqtt callback.  Runs inside check_msg(), so keep it short."""
        print(f'Received on {topic}: {msg}')
        try:
            data = json.loads(msg)
            cmd = data.get('cmd', '')

            # if cmd == 'LASER_ON':
            #     self.turret.laser_on()    # inverted logic (pull-up R on LED)
            #     print('LED turned ON')
            # elif cmd == 'LASER_OFF':
            #     self.turret.laser_off()
            #     print('LED turned OFF')
            
        except Exception as e:
            print('Error parsing message:', e)

    async def _rx_loop(self):
        """Poll the broker forever."""
        pass  # YOUR CODE HERE

    # --- outgoing ------------------------------------------------------------

    def publish_telemetry(self):
        """Send one telemetry packet on the sensor topic."""
        pass  # YOUR CODE HERE

    async def _tx_loop(self):
        """Publish telemetry every PUBLISH_MS forever."""
        pass  # YOUR CODE HERE

    # --- entry point ---------------------------------------------------------

    def start(self):
        """Launch both loops as tasks.  Returns immediately."""
        pass  # YOUR CODE HERE


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
