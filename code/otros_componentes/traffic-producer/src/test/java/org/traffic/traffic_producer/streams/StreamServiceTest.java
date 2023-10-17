package org.traffic.traffic_producer.streams;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
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
import static org.traffic.traffic_producer.streams.StreamService.STREAM_SERVICE_ADDRESS;

@ExtendWith(VertxExtension.class)
@Slf4j
class StreamServiceTest {

  private static JsonObject config;

  private static List<Stream> manyStreams;

  private static Stream oneStream;

  private static StreamRepository mockedRepository;

  @BeforeAll
  static void configure(Vertx vertx) {
    config = new JsonObject(vertx.fileSystem().readFileBlocking("streams/stream-data-source.json"));

    val someStreamsAsJson =
        new JsonObject(vertx.fileSystem().readFileBlocking("streams/sample-streams.json"));
    manyStreams =
        someStreamsAsJson.getJsonArray("list").stream()
            .map(__ -> ((JsonObject) __))
            .map(Stream::fromJson)
            .collect(Collectors.toList());

    val oneStreamAsJson = someStreamsAsJson.getJsonArray("list").getJsonObject(0);
    oneStream = Stream.fromJson(oneStreamAsJson);

    mockedRepository = mock(StreamRepository.class);
    when(mockedRepository.findAll()).thenReturn(Future.succeededFuture(manyStreams));
    when(mockedRepository.find(oneStream.generatedBy, oneStream.feature))
        .thenReturn(Future.succeededFuture(oneStream));
    when(mockedRepository.save(oneStream)).thenReturn(Future.succeededFuture(oneStream));
    when(mockedRepository.saveAll(manyStreams)).thenReturn(Future.succeededFuture(manyStreams));
    when(mockedRepository.deleteAllBy(List.of(oneStream.generatedBy)))
        .thenReturn(Future.succeededFuture(List.of(oneStream)));
  }

  @BeforeEach
  void deploy(Vertx vertx, VertxTestContext testContext) {
    val deploymentOptions = new DeploymentOptions().setWorker(true).setConfig(config);

    val verticle = new StreamService(mockedRepository);
    vertx
        .deployVerticle(verticle, deploymentOptions)
        .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  @DisplayName(
      "When a request to save a batch of streams is made to the repository the result must be"
          + " the same batch of streams requested")
  void saveAllTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val payload =
        new JsonObject()
            .put(
                "list",
                manyStreams.stream().map(Stream::asJson).collect(JsonCollector.toJsonArray()));
    val action = new DeliveryOptions().addHeader("action", "saveAll");
    val expected = manyStreams;

    vertx
        .eventBus()
        .request(STREAM_SERVICE_ADDRESS, payload, action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val actual =
                              ((JsonObject) reply.body())
                                  .getJsonArray("list").stream()
                                      .map(__ -> ((JsonObject) __))
                                      .map(Stream::fromJson)
                                      .collect(Collectors.toList());
                          Assertions.assertEquals(expected, actual);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a request to save a stream is made to the repository the result must be"
          + " the same stream requested")
  void saveTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val payload = Stream.asJson(oneStream);

    val action = new DeliveryOptions().addHeader("action", "save");
    val expected = oneStream;

    vertx
        .eventBus()
        .request(STREAM_SERVICE_ADDRESS, payload, action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val actual = Stream.fromJson(((JsonObject) reply.body()));
                          Assertions.assertEquals(expected, actual);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a request to fetch all streams is made to the repository, the result must"
          + " contain all the streams")
  void findAllTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val expected = manyStreams;
    val action = new DeliveryOptions().addHeader("action", "findAll");

    vertx
        .eventBus()
        .request(STREAM_SERVICE_ADDRESS, null, action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val actual =
                              ((JsonObject) reply.body())
                                  .getJsonArray("list").stream()
                                      .map(__ -> ((JsonObject) __))
                                      .map(Stream::fromJson)
                                      .collect(Collectors.toUnmodifiableList());

                          Assertions.assertEquals(expected, actual);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a request to fetch one stream given the sensorId and feature to "
          + "the repository, the result must be a stream matching the above criteria")
  void findOneTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val sensorId = oneStream.generatedBy;
    val feature = oneStream.feature;
    val action = new DeliveryOptions().addHeader("action", "findOne");
    vertx
        .eventBus()
        .<JsonObject>request(
            STREAM_SERVICE_ADDRESS,
            new JsonObject().put("sensorId", sensorId).put("feature", feature),
            action)
        .onComplete(
            testContext.succeeding(
                reply ->
                    testContext.verify(
                        () -> {
                          val actual = Stream.fromJson(reply.body());
                          Assertions.assertEquals(oneStream, actual);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName("When a request to delete all sensor having an id")
  void deleteAllMatching(Vertx vertx, VertxTestContext testContext) throws Throwable {
    val expected = List.of(oneStream);
    val action = new DeliveryOptions().addHeader("action", "deleteAllBy");
    vertx
        .eventBus()
        .<JsonObject>request(
            STREAM_SERVICE_ADDRESS,
            new JsonObject().put("list", new JsonArray().add(oneStream.generatedBy)),
            action)
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          val actual =
                              result.body().getJsonArray("list").stream()
                                  .map(__ -> (JsonObject) __)
                                  .map(Stream::fromJson)
                                  .collect(Collectors.toList());
                          Assertions.assertEquals(expected, actual);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
