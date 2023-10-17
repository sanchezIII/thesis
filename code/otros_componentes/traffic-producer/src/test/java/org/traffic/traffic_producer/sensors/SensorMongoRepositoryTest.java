package org.traffic.traffic_producer.sensors;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.traffic.traffic_producer.sensors.SensorMongoRepository.COLLECTION_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class SensorMongoRepositoryTest {

  private static SensorMongoRepository repository;

  private static MongoClient mongoClient;

  private static List<Sensor> manySensors;

  @BeforeAll
  static void configure(Vertx vertx) {
    JsonObject config =
        new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sensor-data-source.json"));
    repository = new SensorMongoRepository(vertx, config);
    mongoClient = MongoClient.create(vertx, config);

    JsonArray anArrayOfSensors =
        new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sample-sensors.json"))
            .getJsonArray("list");

    manySensors =
        anArrayOfSensors.stream()
            .map(__ -> ((JsonObject) __))
            .map(Sensor::fromJson)
            .collect(Collectors.toList());
  }

  @AfterAll
  static void tearDown(VertxTestContext testContext) {
    mongoClient.dropCollection(COLLECTION_NAME).onComplete(testContext.succeedingThenComplete());
  }

  @BeforeEach
  void reset(VertxTestContext testContext) {
    mongoClient.dropCollection(COLLECTION_NAME).onComplete(testContext.succeedingThenComplete());
  }

  @Test
  @DisplayName(
      "When a request to save a list of sensors is made it must return the same "
          + " list of sensor succeeding")
  void saveAllTest(VertxTestContext testContext) throws Throwable {
    val expected = manySensors;
    repository
        .saveAll(expected)
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(expected, result);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
