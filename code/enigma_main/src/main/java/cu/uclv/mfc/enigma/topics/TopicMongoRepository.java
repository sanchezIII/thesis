package cu.uclv.mfc.enigma.topics;

import cu.uclv.mfc.enigma.logger.Log;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import java.util.ArrayList;
import java.util.List;

public class TopicMongoRepository implements TopicRepository {

  private final Log log = new Log(TopicMongoRepository.class);

  public static final String COLLECTION_NAME = "topics";

  private final MongoClient client;

  public TopicMongoRepository(Vertx vertx, JsonObject config) {
    client = MongoClient.create(vertx, config);
  }

  @Override
  public Future<Topic> save(Topic topic) {
    Promise<Topic> promise = Promise.promise();

    client
      .insert(COLLECTION_NAME, Topic.asJson(topic))
      .compose(documentId ->
        client.find(COLLECTION_NAME, new JsonObject().put("_id", documentId))
      )
      .map(jsonList -> jsonList.get(0))
      .onSuccess(result -> {
        log.sys("Topic {} has been inserted.", result);
        promise.complete(topic);
      })
      .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<List<Topic>> find(JsonObject query) {
    return client
      .find(COLLECTION_NAME, query)
      .compose(jsonList -> {
        List<Topic> topicList = new ArrayList<>();

        for (JsonObject json : jsonList) {
          try {
            topicList.add(Topic.fromJson(json));
          } catch (ClassNotFoundException e) {
            return Future.failedFuture(
              "Some classes on topics were not found: " + json
            );
          }
        }

        return Future.succeededFuture(topicList);
      });
  }

  @Override
  public Future<Long> count(JsonObject query) {
    return client.count(COLLECTION_NAME, query);
  }
}
