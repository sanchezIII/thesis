package cu.uclv.mfc.enigma.api;

import static org.junit.jupiter.api.Assertions.*;

import cu.uclv.mfc.enigma.sensors.Sensor;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ApiServiceTest {

  private static JsonObject config;

  private static MongoClient mongoClient;

  private static Vertx vertx;

  private static final Sensor sensor = new Sensor(
    "brussels1",
    1.4456,
    -0.2633,
    "speed",
    "km",
    "7c957d63-9770-413a-91d0-66f13eb40d65"
  );

  @BeforeEach
  void setUp(VertxTestContext testContext) throws Throwable {
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

    vertx.deployVerticle(
      new ApiService(new TopicCollection()),
      new DeploymentOptions()
        .setWorker(true)
        .setConfig(config.getJsonObject("api-service"))
    );

    dropAllCollectionsFuture.onComplete(__ -> testContext.completeNow());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  //    @Test
  void testAPI(VertxTestContext testContext) throws Throwable {
    WebClient client = WebClient.create(vertx);

    int port = config.getJsonObject("api-service").getInteger("port");
    String host = "http://localhost";

    client
      .post(port, host, "/addsensor")
      .sendJsonObject(Sensor.asJson(sensor))
      .onComplete(response -> {
        System.out.println(response);
        testContext.succeedingThenComplete();
      });

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
