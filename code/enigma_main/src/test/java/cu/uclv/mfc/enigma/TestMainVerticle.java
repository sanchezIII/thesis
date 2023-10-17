package cu.uclv.mfc.enigma;

import broker.JsonKafkaProducer;
import cu.uclv.mfc.enigma.sensors.Sensor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

  private static JsonObject config;

  private static Vertx vertx;

  private static MongoClient mongoClient;

  @BeforeAll
  static void configure(VertxTestContext testContext) throws Throwable {
    vertx = Vertx.vertx();

    config =
      new JsonObject(vertx.fileSystem().readFileBlocking("conf/config.json"));

    mongoClient =
      MongoClient.create(
        vertx,
        new JsonObject()
          .put("connection_string", "mongodb://localhost:27017")
          .put("db_name", "traffic")
      );

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
  void testJsonKafkaProducer(VertxTestContext testContext) throws Throwable {
    final String TOPIC = "test_topic";

    final JsonObject jsonObject = new JsonObject(new HashMap<>());

    JsonKafkaProducer prod = new JsonKafkaProducer(vertx, "localhost", 9092);

    val result = prod.send(
      TOPIC,
      "mykey",
      new JsonObject()
        .put("nombre1", "luis")
        .put("nombre2", "enrique")
        .put("apellido1", "saborit")
        .put("apellido2", "gonzalez"),
      0
    );

    result.onComplete(testContext.succeedingThenComplete());
    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  private static Set<Sensor> parseSensors(JsonObject sensorsRaw) {
    val sensors = new HashSet<Sensor>();
    sensorsRaw
      .getJsonArray("features")
      .stream()
      .map(__ -> ((JsonObject) __))
      .forEach(raw -> {
        val latitude = raw
          .getJsonObject("geometry")
          .getJsonArray("coordinates")
          .getDouble(0);
        val longitude = raw
          .getJsonObject("geometry")
          .getJsonArray("coordinates")
          .getDouble(1);
        val id = raw.getJsonObject("properties").getString("traverse_name");
      });
    return sensors;
  }
}
