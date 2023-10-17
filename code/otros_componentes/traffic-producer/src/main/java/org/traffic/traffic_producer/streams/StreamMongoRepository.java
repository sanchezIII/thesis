package org.traffic.traffic_producer.streams;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_producer.util.JsonCollector;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Implementacion de la interfaz con la base de datos en MongoDB
 */
@Slf4j
public class StreamMongoRepository implements StreamRepository {

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
        .onSuccess(
            result -> {
              log.info("Stream: {} has been inserted", stream);
              promise.complete(stream);
            })
        .onFailure(
            throwable -> {
              log.error("Unable to insert stream: {}", stream);
              promise.fail(throwable);
            });

    return promise.future();
  }

  @Override
  public Future<List<Stream>> saveAll(List<Stream> streams) {
    val insertions =
        streams.stream()
            .map(Stream::asJson)
            .map(BulkOperation::createInsert)
            .collect(Collectors.toList());
    Promise<List<Stream>> promise = Promise.promise();
    client
        .bulkWrite(COLLECTION_NAME, insertions)
        .onSuccess(
            result -> {
              log.info("{} streams have been inserted", result.getInsertedCount());
              promise.complete(streams);
            })
        .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<Stream> find(@NonNull String sensorId, @NonNull String feature) {
    Promise<Stream> promise = Promise.promise();
    val filter = new JsonObject().put("generatedBy", sensorId).put("feature", feature);
    client
        .findOne(COLLECTION_NAME, filter, null)
        .onSuccess(
            sensorAsJson -> {
              if (sensorAsJson == null) {
                log.warn(
                    "No stream found with sensorId: [{}] and feature: [{}]", sensorId, feature);
                promise.fail("Not found");
              } else {
                promise.complete(Stream.fromJson(sensorAsJson));
              }
            })
        .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<List<Stream>> findAll() {
    return client
        .find(COLLECTION_NAME, new JsonObject())
        .map(list -> list.stream().map(Stream::fromJson).collect(Collectors.toList()));
  }

  @Override
  public Future<List<Stream>> findAll(List<String> sensorIds) {
    val filter =
        new JsonObject()
            .put(
                "generatedBy",
                new JsonObject()
                    .put("$in", sensorIds.stream().collect(JsonCollector.toJsonArray())));
    return client
        .find(COLLECTION_NAME, filter)
        .map(hits -> hits.stream().map(Stream::fromJson).collect(Collectors.toList()));
  }

  @Override
  public Future<List<Stream>> deleteAllBy(List<String> sensorIds) {
    return findAll(sensorIds)
        .compose(
            streams -> {
              Promise<List<Stream>> promise = Promise.promise();
              val ids = streams.stream().map(Stream::getId).collect(JsonCollector.toJsonArray());
              client
                  .removeDocuments(
                      COLLECTION_NAME, new JsonObject().put("id", new JsonObject().put("$in", ids)))
                  .onSuccess(
                      deleteResult -> {
                        log.info("Deleted: [{}] streams", deleteResult.getRemovedCount());
                        promise.complete(streams);
                      })
                  .onFailure(promise::fail);
              return promise.future();
            });
  }
}
