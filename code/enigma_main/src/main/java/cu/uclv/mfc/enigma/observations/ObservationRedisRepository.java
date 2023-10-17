package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicNotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import lombok.val;

public class ObservationRedisRepository extends ObservationRepository {

  private final RedisAPI api;

  public ObservationRedisRepository(
    Vertx vertx,
    JsonObject config,
    TopicCollection topics
  ) {
    super(topics);
    val uri = config.getString("uri");
    api =
      RedisAPI.api(
        Redis.createClient(
          vertx,
          new RedisOptions()
            .setConnectionString(uri)
            .setMaxWaitingHandlers(1000)
            .setMaxPoolWaiting(1000)
        )
      );
  }

  @Override
  public Future<Observation> publish(Observation observation, String topicId) {
    Promise<Observation> promise = Promise.promise();

    String channel = observation.getStreamId();
    ObservationDataFactory factory = null;

    try {
      factory = topics.getTopicById(topicId).getFactory();

      api
        .publish(channel, Observation.asJson(observation, factory).toString())
        .onSuccess(response -> promise.complete(observation))
        .onFailure(promise::fail);
    } catch (TopicNotFoundException e) {
      promise.fail("Topic Not Found");
    }

    return promise.future();
  }
}
