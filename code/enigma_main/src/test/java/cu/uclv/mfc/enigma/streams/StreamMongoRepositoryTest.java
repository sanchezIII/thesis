package cu.uclv.mfc.enigma.streams;

import static cu.uclv.mfc.enigma.streams.StreamMongoRepository.COLLECTION_NAME;
import static org.junit.jupiter.api.Assertions.*;

import cu.uclv.mfc.enigma.sensors.Sensor;
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
class StreamMongoRepositoryTest {

  private static JsonObject config;

  private static MongoClient mongoClient;

  private static StreamMongoRepository repository;

  private static final JsonObject streamJson = new JsonObject(
    "{" +
    "\"id\":\"count-stream-1\"," +
    "\"location\":{" +
    "\"latitude\":1.23666," +
    "\"longitude\": -0.23666" +
    "}," +
    "\"streamStart\":\"2021/09/05 01:25\"," +
    "\"generatedBy\":\"brussels-1\"," +
    "\"feature\":\"count\"" +
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

    repository =
      new StreamMongoRepository(vertx, config.getJsonObject("stream-service"));

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
      .save(Stream.fromJson(streamJson))
      .map(Stream::asJson)
      .compose(savedStream -> {
        assertEquals(
          streamJson,
          savedStream,
          "El devuelto por el metodo debe ser igual al original"
        );
        return Future.succeededFuture(savedStream);
      })
      .compose(savedStream ->
        mongoClient
          .find(
            COLLECTION_NAME,
            new JsonObject().put("id", streamJson.getString("id"))
          )
          .compose(streamList -> {
            Assertions.assertEquals(1, streamList.size());

            val retrievedStream = streamList.get(0);
            retrievedStream.remove("_id");

            assertEquals(
              streamJson,
              retrievedStream,
              "El sensor en el repositorio debe ser igual al original"
            );
            return Future.succeededFuture();
          })
      )
      .onComplete(testContext.succeedingThenComplete());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
