package org.traffic.traffic_producer.observations;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.traffic.traffic_producer.util.JsonCollector;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.traffic.traffic_producer.observations.ObservationService.OBSERVATION_SERVICE_ADDRESS;

@ExtendWith(VertxExtension.class)
@Slf4j
class ObservationServiceTest {

  private static JsonObject config;

  private static Observation oneObservation;

  private static List<Observation> manyObservations;

  private static ObservationRepository mockedRepository;

  @BeforeAll
  static void loadConfig(Vertx vertx) {
    config =
        new JsonObject(
            vertx.fileSystem().readFileBlocking("observations/observation-data-sink.json"));

    oneObservation = new Observation("obs-11", "streamId1", 42.66, LocalDateTime.now());

    manyObservations =
        List.of(
            new Observation("s1", 50, LocalDateTime.now()),
            new Observation("s2", 23.66, LocalDateTime.now()),
            new Observation("s3", 1000, LocalDateTime.now()));

    mockedRepository = mock(ObservationRepository.class);
    when(mockedRepository.publish(oneObservation))
        .thenReturn(Future.succeededFuture(oneObservation));

    manyObservations.forEach(
        observation ->
            when(mockedRepository.publish(observation))
                .thenReturn(Future.succeededFuture(observation)));
  }

  @BeforeEach
  void deployVerticle(Vertx vertx, VertxTestContext testContext) {
    val deploymentOptions = new DeploymentOptions().setWorker(true).setConfig(config);
    val verticle = new ObservationService(mockedRepository);
    vertx
        .deployVerticle(verticle, deploymentOptions)
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  @DisplayName(
      "When a request to publish an observation is made to the service, the same observation"
          + " must be returned if no errors happened")
  void publishTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val givenObservation = oneObservation;
    val payload = Observation.asJson(givenObservation);
    val action = new DeliveryOptions().addHeader("action", "publish");

    vertx
        .eventBus()
        .request(OBSERVATION_SERVICE_ADDRESS, payload, action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          assertEquals(
                              givenObservation, Observation.fromJson(((JsonObject) reply.body())));
                          testContext.completeNow();
                        })));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  @DisplayName(
      "When a request to publish many observation is made to the service, the same observations"
          + " must be returned if no errors happened")
  void publishAllTest(Vertx vertx, VertxTestContext testContext) throws Throwable {

    val action = new DeliveryOptions().addHeader("action", "publishAll");

    val expected = manyObservations;
    val payload =
        new JsonObject()
            .put(
                "list",
                expected.stream().map(Observation::asJson).collect(JsonCollector.toJsonArray()));

    vertx
        .eventBus()
        .request(OBSERVATION_SERVICE_ADDRESS, payload, action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val actual =
                              ((JsonObject) reply.body())
                                  .getJsonArray("list").stream()
                                      .map(__ -> ((JsonObject) __))
                                      .map(Observation::fromJson)
                                      .collect(Collectors.toList());
                          assertEquals(expected, actual);
                          testContext.completeNow();
                        })));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }
}
