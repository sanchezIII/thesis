package cu.uclv.mfc.enigma.sensors;

import static org.junit.jupiter.api.Assertions.*;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SensorTest {

  private static final Sensor sensor = new Sensor(
    "brussels1",
    1.4456,
    -0.2633,
    "speed",
    "km",
    "7c957d63-9770-413a-91d0-66f13eb40d65"
  );

  private static final JsonObject sensorJson = new JsonObject(
    "{" +
    "\"id\": \"brussels1\"," +
    "\"unit\": \"km\"," +
    "\"quantityKind\": \"speed\"," +
    "\"latitude\": 1.4456," +
    "\"longitude\": -0.2633," +
    "\"topic\":\"7c957d63-9770-413a-91d0-66f13eb40d65\"" +
    "}"
  );

  @Test
  void asJson() {
    Assertions.assertEquals(Sensor.asJson(sensor), sensorJson);
  }

  @Test
  void fromJson() {
    Assertions.assertEquals(Sensor.fromJson(sensorJson), sensor);
  }
}
