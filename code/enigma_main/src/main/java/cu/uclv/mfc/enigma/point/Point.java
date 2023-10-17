package cu.uclv.mfc.enigma.point;

import io.vertx.core.json.JsonObject;
import lombok.Value;

@Value
public class Point {

  double latitude;
  double longitude;

  public static JsonObject asJson(Point point) {
    return new JsonObject()
      .put("latitude", point.latitude)
      .put("longitude", point.longitude);
  }

  public static Point fromJson(JsonObject json) {
    return new Point(json.getDouble("latitude"), json.getDouble("longitude"));
  }
}
