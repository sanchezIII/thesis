package org.traffic.traffic_producer.sensors;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {

  String id;
  double latitude;
  double longitude;
  String quantityKind;
  String unit;

  public static JsonObject asJson(Sensor sensor) {
    return JsonObject.mapFrom(sensor);
  }

  public static Sensor fromJson(JsonObject sensorAsJson) {
    return Json.decodeValue(sensorAsJson.toBuffer(), Sensor.class);
  }
}
