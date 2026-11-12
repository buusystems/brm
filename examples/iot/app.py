"""IoT example: sites -> devices -> sensors -> readings.

    pip install -e '.[mqtt]'
    BRM_MQTT=localhost uvicorn examples.iot.app:app --reload
    open http://127.0.0.1:8000/docs
"""

import logging
import os
from pathlib import Path

from brm import App
from brm.handlers import MQTT, log, when

logging.basicConfig(level=logging.INFO)
HERE = Path(__file__).parent

app = App(os.environ.get("BRM_DB", "iot.db"), schema=str(HERE / "schema.sql"))

# Every change anywhere under a site (device, sensor, reading) is logged at that site.
app.on("site", log(), watch=["self", "descendants"])


# A reading over its sensor's threshold: the walk goes reading -> sensor (depth 1).
@app.on("sensor", watch="descendants", source="reading", ops="insert", depth=1)
def over_threshold(ev):
    value, limit = ev.change.new["value"], ev.row["max_value"]
    if limit is not None and value > limit:
        logging.warning("sensor %s (%s) read %s > %s", ev.row["id"], ev.row["kind"], value, limit)


# Taking a device offline tells every sensor below it (walk down: device -> sensors).
app.on("sensor", lambda ev: logging.info("sensor %s: device %s went offline",
                                         ev.row["id"], ev.change.key[0]),
       watch="ancestors", source="device", ops="update",
       where=lambda ev: ev.change.new["online"] == 0)

if os.environ.get("BRM_MQTT"):
    bus = MQTT(os.environ["BRM_MQTT"], int(os.environ.get("BRM_MQTT_PORT", 1883)))
    # ping over MQTT on brm/device/<id> whenever anything below a device changes
    app.on("device", bus.publish("brm/device/{key}"), watch="descendants", retries=3)
    # and on readings over threshold, a dedicated alert topic
    app.on("sensor", when(lambda ev: ev.row["max_value"] is not None
                          and ev.change.new["value"] > ev.row["max_value"],
                          bus.publish("brm/alert/sensor/{key}")),
           watch="descendants", source="reading", ops="insert", depth=1, retries=3)
