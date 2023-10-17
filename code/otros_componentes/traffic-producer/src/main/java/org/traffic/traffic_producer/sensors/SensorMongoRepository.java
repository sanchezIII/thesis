package org.traffic.traffic_producer.sensors;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SensorMongoRepository implements SensorRepository {

  public static final String COLLECTION_NAME = "sensors";

  private final MongoClient client;

  public SensorMongoRepository(Vertx vertx, JsonObject config) {
    client = MongoClient.create(vertx, config);
  }

  @Override
  public Future<List<Sensor>> saveAll(List<Sensor> sensors) {
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
