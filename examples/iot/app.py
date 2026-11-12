"""IoT example: sites -> devices -> sensors -> readings.

    pip install -e '.[mqtt]'
    BRM_MQTT=localhost uvicorn examples.iot.app:app --reload
    open http://127.0.0.1:8000/docs
"""

import logging
import os
from pathlib import Path

from brm import App
from brm.handlers import MQTT, log

logging.basicConfig(level=logging.INFO)
HERE = Path(__file__).parent

app = App(os.environ.get("BRM_DB", "iot.db"), schema=str(HERE / "schema.sql"))

# Every change anywhere under a site (device, sensor, reading) is logged at that site.
app.on("site", log())
app.on("site", log(), via=["device", "sensor", "reading"])


def over(ev) -> bool:
    """A reading above its sensor's threshold (ev.row is the sensor)."""
    return ev.row["max_value"] is not None and ev.change.new["value"] > ev.row["max_value"]


# A new reading of this sensor (exactly reading.sensor_id) over the threshold. Checked
# in the write's transaction, so readings under the threshold queue nothing.
@app.on("sensor", via="reading.sensor_id", ops="insert", where=over)
def over_threshold(ev):
    logging.warning("sensor %s (%s) read %s > %s", ev.row["id"], ev.row["kind"],
                    ev.change.new["value"], ev.row["max_value"])


# Taking a device offline tells every sensor below it (device is above sensor: walks down).
app.on("sensor", lambda ev: logging.info("sensor %s: device %s went offline",
                                         ev.row["id"], ev.change.key[0]),
       via="device", ops="update", name="sensor:device_offline",
       where=lambda ev: ev.change.new["online"] == 0)

if os.environ.get("BRM_MQTT"):
    bus = MQTT(os.environ["BRM_MQTT"], int(os.environ.get("BRM_MQTT_PORT", 1883)))
    # ping over MQTT on brm/device/<id> whenever anything below a device changes
    app.on("device", bus.publish("brm/device/{key}"), via=["sensor", "reading"], retries=3)
    # and on readings over threshold, a dedicated alert topic
    app.on("sensor", bus.publish("brm/alert/sensor/{key}"), via="reading.sensor_id",
           ops="insert", where=over, retries=3, name="sensor:alert")
