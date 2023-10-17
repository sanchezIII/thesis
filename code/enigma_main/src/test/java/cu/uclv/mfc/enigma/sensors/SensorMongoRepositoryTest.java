package cu.uclv.mfc.enigma.sensors;

import static cu.uclv.mfc.enigma.sensors.SensorMongoRepository.COLLECTION_NAME;
import static org.junit.jupiter.api.Assertions.*;

import cu.uclv.mfc.enigma.streams.Stream;
import cu.uclv.mfc.enigma.streams.StreamMongoRepository;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class SensorMongoRepositoryTest {

  private static JsonObject config;

  private static MongoClient mongoClient;

  private static SensorMongoRepository repository;

  private static final JsonObject sensorJson = new JsonObject(
    "{" +
    "\"id\": \"brussels1\"," +
    "\"unit\": \"km\"," +
    "\"quantityKind\": \"speed\"," +
    "\"latitude\": 1.4456," +
    "\"longitude\": -0.2633," +
    "\"topic\":\"7c957d63-9770-413a-91d0-66f13eb40d65\"" +
    "}"
  );

  @BeforeAll
  static void configure(VertxTestContext testContext) throws Throwable {
    Vertx vertx = Vertx.vertx();

    config =
      new JsonObject(vertx.fileSystem().readFileBlocking("conf/config.json"));

    mongoClient =
      MongoClient.create(
        vertx,
        new JsonObject()
          .put("connection_string", "mongodb://localhost:27017")
          .put("db_name", "traffic")
      );

    val sensorConfig = config.getJsonObject("sensor-service");

    System.out.println(sensorConfig);

    repository = new SensorMongoRepository(vertx, sensorConfig);

    val dropAllCollectionsFuture = mongoClient
      .getCollections()
      .compose(stringList ->
        CompositeFuture.all(
          stringList
            .stream()
            .map(string -> mongoClient.dropCollection(string))
            .collect(Collectors.toList())
        )
      );

    dropAllCollectionsFuture.onComplete(testContext.succeedingThenComplete());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  void test(VertxTestContext testContext) throws Throwable {
    repository
      .save(Sensor.fromJson(sensorJson))
      .map(Sensor::asJson)
      .compose(savedSensor -> {
        assertEquals(
          sensorJson,
          savedSensor,
          "El devuelto por el metodo debe ser igual al original"
        );
        return Future.succeededFuture(savedSensor);
      })
      .compose(savedSensor ->
        mongoClient
          .find(
            COLLECTION_NAME,
            new JsonObject().put("id", sensorJson.getString("id"))
          )
          .compose(sensorList -> {
            Assertions.assertEquals(1, sensorList.size());

            val retrievedSensor = sensorList.get(0);
            retrievedSensor.remove("_id");

            assertEquals(
              sensorJson,
              retrievedSensor,
              "El sensor en el repositorio debe ser igual al original"
            );
            return Future.succeededFuture();
          })
      )
      .onComplete(testContext.succeedingThenComplete());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
