package cu.uclv.mfc.enigma.sensors;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.List;

public interface SensorRepository {
  Future<List<Sensor>> saveAll(List<Sensor> sensor);

  Future<Sensor> save(Sensor sensor);

  Future<List<Sensor>> find(JsonObject query);
}
