package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicNotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class ObservationMongoRepository extends ObservationRepository {

  public static final String COLLECTION_NAME = "observations";
  public static Log log = new Log(ObservationMongoRepository.class);

  private final MongoClient client;

  public ObservationMongoRepository(
    Vertx vertx,
    JsonObject config,
    TopicCollection topics
  ) {
    super(topics);
    client = MongoClient.create(vertx, config);
  }

  @Override
  public Future<Observation> publish(Observation observation, String topicId) {
    Promise<Observation> promise = Promise.promise();

    ObservationDataFactory factory = null;
    try {
      factory = topics.getTopicById(topicId).getFactory();

      client
        .insert(COLLECTION_NAME, Observation.asJson(observation, factory))
        .onSuccess(result -> {
          log.sys("Observation {} has been inserted.", observation.toString());
          promise.complete(observation);
        })
        .onFailure(promise::fail);
    } catch (TopicNotFoundException e) {
      promise.fail(e.getMessage());
    }

    return promise.future();
  }
}
