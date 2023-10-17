package org.traffic.traffic_producer.streams;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
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

import static org.traffic.traffic_producer.streams.StreamMongoRepository.COLLECTION_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class StreamMongoRepositoryTest {

  private static StreamMongoRepository repository;

  private static MongoClient mongoClient;

  private static JsonObject oneStream;

  private static JsonObject anotherStream;

  private static JsonObject yetAnotherStream;

  @BeforeAll
  static void configure(Vertx vertx) {
    JsonObject config =
        new JsonObject(vertx.fileSystem().readFileBlocking("streams/stream-data-source.json"));
    repository = new StreamMongoRepository(vertx, config);
    mongoClient = MongoClient.create(vertx, config);

    JsonArray anArrayOfStreams =
        new JsonObject(vertx.fileSystem().readFileBlocking("streams/sample-streams.json"))
            .getJsonArray("list");
    oneStream = anArrayOfStreams.getJsonObject(0);
    anotherStream = anArrayOfStreams.getJsonObject(1);
    yetAnotherStream = anArrayOfStreams.getJsonObject(2);
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
      "When saving a stream and then querying the database, the same stream must be" + " inserted")
  void saveTest(VertxTestContext testContext) throws Throwable {

    val streamReceivedCheckpoint = testContext.checkpoint();
    val streamInDatabaseCheckpoint = testContext.checkpoint();

    val expected = Stream.fromJson(oneStream);
    repository
        .save(expected)
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(expected, result);
                          streamReceivedCheckpoint.flag();
                        })))
        .compose(stream -> mongoClient.findOne(COLLECTION_NAME, oneStream, null))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(expected, Stream.fromJson(result));
                          streamInDatabaseCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When saveAll request is made to the repository with N documents, then N documents"
          + " must exists in the database after the saveAll operations finishes")
  void saveAllTest(VertxTestContext testContext) throws Throwable {

    val listOfStreamReceivedCheckpoint = testContext.checkpoint();
    val listOfStreamInDatabaseCheckpoint = testContext.checkpoint();

    val expected = List.of(Stream.fromJson(oneStream), Stream.fromJson(anotherStream));
    repository
        .saveAll(expected)
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(expected, result);
                          listOfStreamReceivedCheckpoint.flag();
                        })))
        .compose(stream -> mongoClient.find(COLLECTION_NAME, new JsonObject()))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(
                              expected,
                              result.stream().map(Stream::fromJson).collect(Collectors.toList()));
                          listOfStreamInDatabaseCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a find request is made to the repository and there is a matching document"
          + " with the specified criteria, that document must be returned")
  void findTest(VertxTestContext testContext) throws Throwable {

    val insertedCheckpoint = testContext.checkpoint();
    val foundCheckpoint = testContext.checkpoint();
    val expected = Stream.fromJson(oneStream);

    mongoClient
        .insert(COLLECTION_NAME, oneStream)
        .onComplete(testContext.succeeding(result -> testContext.verify(insertedCheckpoint::flag)))
        .compose(__ -> repository.find(expected.generatedBy, expected.feature))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(expected, result);
                          foundCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a find request is made to the repository and there is no matching document"
          + " with the specified criteria, then a failed future must be returned")
  void findWhenNoMatchTest(VertxTestContext testContext) throws Throwable {

    repository.find("", "").onComplete(testContext.failingThenComplete());

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a findAll request is made to the repository and there are two documents"
          + " in the database, then two documents must be returned")
  void findAllTest(VertxTestContext testContext) throws Throwable {

    val insertedCheckpoint = testContext.checkpoint();
    val foundCheckpoint = testContext.checkpoint();
    val expected = List.of(Stream.fromJson(oneStream), Stream.fromJson(anotherStream));

    mongoClient
        .bulkWrite(
            COLLECTION_NAME,
            expected.stream()
                .map(Stream::asJson)
                .map(BulkOperation::createInsert)
                .collect(Collectors.toList()))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          log.info("Inserted: [{}] streams", result.getInsertedCount());
                          insertedCheckpoint.flag();
                        })))
        .compose(__ -> repository.findAll())
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          log.info("Found: [{}] streams", result.size());
                          result.forEach(stream -> log.info("Stream: {}", stream));
                          Assertions.assertEquals(expected, result);
                          foundCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When the database is empty and a findAll request is made to the repository"
          + " then an empty list must be returned")
  void findAllWhenEmptyTest(VertxTestContext testContext) throws Throwable {

    val expected = List.<Stream>of();

    repository
        .findAll()
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          log.info("Found: [{}] streams", result.size());
                          result.forEach(stream -> log.info("Stream: {}", stream));
                          Assertions.assertEquals(expected, result);
                          testContext.completeNow();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a findAll request is made to the repository with two sensorIds and within"
          + " the collection only two sensors contain those sensor ids then "
          + " two documents must be returned")
  void findAllMatchingTest(VertxTestContext testContext) throws Throwable {

    val insertedCheckpoint = testContext.checkpoint();
    val foundCheckpoint = testContext.checkpoint();
    val expected = List.of(Stream.fromJson(oneStream), Stream.fromJson(anotherStream));
    val toBeInserted =
        List.of(
            Stream.fromJson(oneStream),
            Stream.fromJson(anotherStream),
            Stream.fromJson(yetAnotherStream));

    mongoClient
        .bulkWrite(
            COLLECTION_NAME,
            toBeInserted.stream()
                .map(Stream::asJson)
                .map(BulkOperation::createInsert)
                .collect(Collectors.toList()))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(toBeInserted.size(), result.getInsertedCount());
                          insertedCheckpoint.flag();
                        })))
        .compose(
            __ ->
                repository.findAll(
                    expected.stream().map(Stream::getGeneratedBy).collect(Collectors.toList())))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          log.info("Found: [{}] streams", result.size());
                          result.forEach(stream -> log.info("Stream: {}", stream));
                          Assertions.assertEquals(expected, result);
                          foundCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName(
      "When a deleteAll request is made to the repository with one sensorId and within"
          + " the collection two sensors contain that sensorId then "
          + " two documents must dissapear  from the database and those documents must be"
          + " returned")
  void deleteAllMatchingTest(Vertx vertx, VertxTestContext testContext) throws Throwable {

    val insertedCheckpoint = testContext.checkpoint();
    val foundCheckpoint = testContext.checkpoint();
    val deletedCheckpoint = testContext.checkpoint();

    val toBeInserted =
        List.of(
            Stream.fromJson(oneStream),
            Stream.fromJson(oneStream),
            Stream.fromJson(anotherStream),
            Stream.fromJson(yetAnotherStream));

    mongoClient
        .bulkWrite(
            COLLECTION_NAME,
            toBeInserted.stream()
                .map(Stream::asJson)
                .map(BulkOperation::createInsert)
                .collect(Collectors.toList()))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertEquals(toBeInserted.size(), result.getInsertedCount());
                          insertedCheckpoint.flag();
                        })))
        .compose(
            __ ->
                repository.deleteAllBy(
                    java.util.stream.Stream.of(Stream.fromJson(oneStream))
                        .map(Stream::getGeneratedBy)
                        .collect(Collectors.toList())))
        .map(
            result -> {
              log.info("Found: [{}] streams", result.size());
              Assertions.assertEquals(2, result.size());
              foundCheckpoint.flag();
              return result;
            })
        .compose(
            __ ->
                mongoClient.find(
                    COLLECTION_NAME,
                    new JsonObject().put("sensorId", Stream.fromJson(oneStream).getGeneratedBy())))
        .onComplete(
            testContext.succeeding(
                result ->
                    testContext.verify(
                        () -> {
                          Assertions.assertTrue(result.isEmpty());
                          deletedCheckpoint.flag();
                        })));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
