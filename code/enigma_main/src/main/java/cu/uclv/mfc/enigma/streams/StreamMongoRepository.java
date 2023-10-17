package cu.uclv.mfc.enigma.streams;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.util.JsonCollector;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * Implementacion de la interfaz con la base de datos en MongoDB
 */
public class StreamMongoRepository implements StreamRepository {

  private final Log log = new Log(StreamMongoRepository.class);

  public static final String COLLECTION_NAME = "streams";

  private final MongoClient client;

  public StreamMongoRepository(Vertx vertx, JsonObject config) {
    client = MongoClient.create(vertx, config);
  }

  @Override
  public Future<Stream> save(Stream stream) {
    Promise<Stream> promise = Promise.promise();

    client
      .insert(COLLECTION_NAME, Stream.asJson(stream))
      .onSuccess(result -> {
        log.sys("Stream has been inserted: {}", stream.toString());
        promise.complete(stream);
      })
      .onFailure(throwable -> {
        log.error("Unable to insert stream: %s", stream);
        promise.fail(throwable);
      });

    return promise.future();
  }

  @Override
  public Future<List<Stream>> saveAll(List<Stream> streams) {
    val insertions = streams
      .stream()
      .map(Stream::asJson)
      .map(BulkOperation::createInsert)
      .collect(Collectors.toList());

    Promise<List<Stream>> promise = Promise.promise();

    client
      .bulkWrite(COLLECTION_NAME, insertions)
      .onSuccess(result -> {
        log.sys("{} streams have been inserted", result.getInsertedCount());
        promise.complete(streams);
      })
      .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<Stream> find(
    @NonNull String sensorId,
    @NonNull String feature
  ) {
    Promise<Stream> promise = Promise.promise();
    val filter = new JsonObject()
      .put("generatedBy", sensorId)
      .put("feature", feature);
    client
      .findOne(COLLECTION_NAME, filter, null)
      .onSuccess(sensorAsJson -> {
        if (sensorAsJson == null) {
          log.warn(
            "No stream found with sensorId: [{}] and feature: [{}]",
            sensorId,
            feature
          );
          promise.fail("Not found");
        } else {
          promise.complete(Stream.fromJson(sensorAsJson));
        }
      })
      .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<List<Stream>> find(JsonObject query) {
    return client
      .find(COLLECTION_NAME, query)
      .map(jsonList ->
        jsonList
          .stream()
          .map(json -> Stream.fromJson(json))
          .collect(Collectors.toList())
      );
  }

  @Override
  public Future<List<Stream>> findAll() {
    return client
      .find(COLLECTION_NAME, new JsonObject())
      .map(list ->
        list.stream().map(Stream::fromJson).collect(Collectors.toList())
      );
  }

  @Override
  public Future<List<Stream>> findAll(List<String> sensorIds) {
    val filter = new JsonObject()
      .put(
        "generatedBy",
        new JsonObject()
          .put("$in", sensorIds.stream().collect(JsonCollector.toJsonArray()))
      );
    return client
      .find(COLLECTION_NAME, filter)
      .map(hits ->
        hits.stream().map(Stream::fromJson).collect(Collectors.toList())
      );
  }

  @Override
  public Future<List<Stream>> deleteAllBy(List<String> sensorIds) {
    return findAll(sensorIds)
      .compose(streams -> {
        Promise<List<Stream>> promise = Promise.promise();
        val ids = streams
          .stream()
          .map(Stream::getId)
          .collect(JsonCollector.toJsonArray());
        client
          .removeDocuments(
            COLLECTION_NAME,
            new JsonObject().put("id", new JsonObject().put("$in", ids))
          )
          .onSuccess(deleteResult -> {
            log.sys("Deleted: [{}] streams", deleteResult.getRemovedCount());
            promise.complete(streams);
          })
          .onFailure(promise::fail);
        return promise.future();
      });
  }
}
