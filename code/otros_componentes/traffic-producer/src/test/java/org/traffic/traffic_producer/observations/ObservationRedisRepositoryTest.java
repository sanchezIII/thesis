package org.traffic.traffic_producer.observations;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(VertxExtension.class)
class ObservationRedisRepositoryTest {

  @Test
  @DisplayName("Publish correctly an observation an returns the published observation")
  void publishTest(Vertx vertx, VertxTestContext testContext) {
    val config =
        new JsonObject(
            vertx.fileSystem().readFileBlocking("observations/observation-data-sink.json"));
    val repository = new ObservationRedisRepository(vertx, config);
    val observation = new Observation("stream-id1", 53.6, "2021/08/27 01:20");
    repository
        .publish(observation)
        .onComplete(
            testContext.succeeding(
                receivedObservation ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(observation, receivedObservation);
                          testContext.completeNow();
                        })));
  }
}
