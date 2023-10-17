package org.traffic.traffic_producer.sensors;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.traffic.traffic_producer.util.JsonCollector;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.traffic.traffic_producer.sensors.SensorService.SENSOR_SERVICE_ADDRESS;

@Slf4j
@ExtendWith(VertxExtension.class)
class SensorServiceTest {

  private static JsonObject config;

  private static List<Sensor> manySensors;

  private static SensorRepository mockedRepository;

  @BeforeAll
  static void configure(Vertx vertx) {
    config = new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sensor-data-source.json"));

    val sampleSensors =
        new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sample-sensors.json"));

    manySensors =
        sampleSensors.getJsonArray("list").stream()
            .map(__ -> ((JsonObject) __))
            .map(Sensor::fromJson)
            .collect(Collectors.toList());

    mockedRepository = mock(SensorRepository.class);
    when(mockedRepository.saveAll(manySensors)).thenReturn(Future.succeededFuture(manySensors));
  }

  @BeforeEach
  void deploy(Vertx vertx, VertxTestContext testContext) {
    val deploymentOptions = new DeploymentOptions().setWorker(true).setConfig(config);
    val verticle = new SensorService(mockedRepository);
    vertx
        .deployVerticle(verticle, deploymentOptions)
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  @DisplayName(
      "When a batch of sensors is sent to the service, a json array with all the previous sensor must be received")
  void saveAllTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val action = new DeliveryOptions().addHeader("action", "saveAll");
    val expected = manySensors;

    vertx
        .eventBus()
        .request(
            SENSOR_SERVICE_ADDRESS,
            new JsonObject()
                .put(
                    "list",
                    manySensors.stream().map(Sensor::asJson).collect(JsonCollector.toJsonArray())),
            action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val received =
                              ((JsonObject) reply.body())
                                  .getJsonArray("list").stream()
                                      .map(__ -> ((JsonObject) __))
                                      .map(Sensor::fromJson)
                                      .collect(Collectors.toList());
                          Assertions.assertEquals(expected, received);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
