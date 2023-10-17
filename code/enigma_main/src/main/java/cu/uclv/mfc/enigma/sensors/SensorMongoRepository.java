package cu.uclv.mfc.enigma.sensors;

import cu.uclv.mfc.enigma.logger.Log;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import java.util.List;
import java.util.stream.Collectors;
import lombok.val;

public class SensorMongoRepository implements SensorRepository {

  public static final String COLLECTION_NAME = "sensors";
  private final Log log = new Log(SensorMongoRepository.class);
  private final MongoClient client;

  public SensorMongoRepository(Vertx vertx, JsonObject config) {
    client = MongoClient.create(vertx, config);
  }

  @Override
  public Future<List<Sensor>> saveAll(List<Sensor> sensors) {
    Promise<List<Sensor>> promise = Promise.promise();

    val insertions = sensors
      .stream()
      .map(Sensor::asJson)
      .map(BulkOperation::createInsert)
      .collect(Collectors.toList());

    client
      .bulkWrite(COLLECTION_NAME, insertions)
      .onSuccess(result -> {
        log.sys("{} sensors have been inserted.", result.getInsertedCount());
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
      .onSuccess(result -> {
        log.sys("Sensor {} has been inserted.", sensor);
        promise.complete(sensor);
      })
      .onFailure(promise::fail);
    return promise.future();
  }

  @Override
  public Future<List<Sensor>> find(JsonObject query) {
    return client
      .find(COLLECTION_NAME, query)
      .compose(jsonList ->
        Future.succeededFuture(
          jsonList
            .stream()
            .map(json -> Sensor.fromJson(json))
            .collect(Collectors.toList())
        )
      );
  }
}
