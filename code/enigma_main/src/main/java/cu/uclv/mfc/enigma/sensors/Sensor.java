package cu.uclv.mfc.enigma.sensors;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {

  private String id;
  private double latitude;
  private double longitude;
  private String quantityKind;
  private String unit;
  private String topic;

  public static JsonObject asJson(Sensor sensor) {
    return JsonObject.mapFrom(sensor);
  }

  public static Sensor fromJson(JsonObject json) {
    return new Sensor(
      json.getString("id"),
      json.getDouble("latitude"),
      json.getDouble("longitude"),
      json.getString("quantityKind"),
      json.getString("unit"),
      json.getString("topic")
    );
  }
}
