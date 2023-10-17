package cu.uclv.mfc.enigma.util;

import io.vertx.core.json.JsonArray;
import java.util.stream.Collector;

public final class JsonCollector {

  public static Collector<Object, JsonArray, JsonArray> toJsonArray() {
    return Collector.of(JsonArray::new, JsonArray::add, JsonArray::add);
  }
}
