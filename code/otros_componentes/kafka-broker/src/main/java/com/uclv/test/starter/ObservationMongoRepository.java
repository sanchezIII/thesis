package com.uclv.test.starter;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

public class ObservationMongoRepository {
  public static final String COLLECTION_NAME = "observations";

  private final MongoClient client;

  public ObservationMongoRepository(Vertx vertx, JsonObject config) {
    client = MongoClient.create(vertx, config);
  }

  public Future<List<>> saveAll(List<Sensor> sensors) {
    val insertions =
        sensors.stream()
            .map(Sensor::asJson)
            .map(BulkOperation::createInsert)
            .collect(Collectors.toList());
    Promise<List<Sensor>> promise = Promise.promise();
    client
        .bulkWrite(COLLECTION_NAME, insertions)
        .onSuccess(
            result -> {
              log.info("{} sensors have been inserted", result.getInsertedCount());
              promise.complete(sensors);
            })
        .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<Sensor> save(Sensor sensor) {
    Promise<Sensor> promise = Promise.promise();

    client
        .insert(COLLECTION_NAME, Sensor.asJson(sensor))
        .onSuccess(
            result -> {
              log.info("Sensor: {} has been inserted", sensor);
              promise.complete(sensor);
            })
        .onFailure(promise::fail);
    return promise.future();
  }

}
