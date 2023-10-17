package cu.uclv.mfc.enigma.streams;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.List;

/**
 * Interfaz con la base de datos
 */
public interface StreamRepository {
  Future<Stream> save(Stream stream);

  Future<List<Stream>> saveAll(List<Stream> streamList);

  Future<Stream> find(String sensorId, String feature);

  Future<List<Stream>> find(JsonObject query);

  Future<List<Stream>> findAll();

  Future<List<Stream>> findAll(List<String> sensorIds);

  Future<List<Stream>> deleteAllBy(List<String> sensorIds);
}
