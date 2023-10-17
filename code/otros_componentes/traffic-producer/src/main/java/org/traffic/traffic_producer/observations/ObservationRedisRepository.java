package org.traffic.traffic_producer.observations;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class ObservationRedisRepository implements ObservationRepository {

  private final RedisAPI api;

  public ObservationRedisRepository(Vertx vertx, JsonObject config) {
    val uri = config.getString("uri");
    api =
        RedisAPI.api(
            Redis.createClient(
                vertx,
                new RedisOptions()
                    .setConnectionString(uri)
                    .setMaxWaitingHandlers(1000)
                    .setMaxPoolWaiting(1000)));
  }

  @Override
  public Future<Observation> publish(Observation observation) {
    String channel = observation.getStreamId();
    Promise<Observation> promise = Promise.promise();
    api.publish(channel, Observation.asJson(observation).toString())
//      .compose(result -> {
//        log.info("Published in {} ", channel);
//
//        return Future.succeededFuture(result);
//      })
        .onSuccess(response -> promise.complete(observation))
        .onFailure(promise::fail);
    return promise.future();
  }
}
