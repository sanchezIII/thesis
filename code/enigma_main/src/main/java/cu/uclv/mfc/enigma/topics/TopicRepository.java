package cu.uclv.mfc.enigma.topics;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.List;

public interface TopicRepository {
  Future<Topic> save(Topic topic);

  Future<List<Topic>> find(JsonObject query);

  Future<Long> count(JsonObject query);
}
