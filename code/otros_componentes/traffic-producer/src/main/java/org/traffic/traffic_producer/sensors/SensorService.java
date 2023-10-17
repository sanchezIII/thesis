package org.traffic.traffic_producer.sensors;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
public final class SensorService extends AbstractVerticle {

  public static final String SENSOR_SERVICE_ADDRESS = "sensor-service";
  private SensorRepository repository;
  private MessageConsumer<JsonObject> consumer;

  public SensorService(SensorRepository repository) {
    this.repository = repository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository = new SensorMongoRepository(vertx, context.config());
  }

  @Override
  public void start() {
    consumer =
        vertx
            .eventBus()
            .consumer(
                SENSOR_SERVICE_ADDRESS,
                message -> {
                  log.info(message.body().toString());

                  switch (message.headers().get("action")) {
                    case "saveAll":
                      val sensors =
                          message.body().getJsonArray("list").stream()
                              .map(__ -> (JsonObject) __)
                              .map(Sensor::fromJson)
                              .collect(Collectors.toList());
                      repository
                          .saveAll(sensors)
                          .onSuccess(
                              savedSensors ->
                                  message.reply(
                                      new JsonObject()
                                          .put("list", message.body().getJsonArray("list"))))
                          .onFailure(throwable -> message.fail(500, "Unable to save sensors"));
                    case "save":
                      Sensor sensor = Sensor.fromJson(message.body());

                      repository
                        .save(sensor)
                        .onSuccess(
                          savedSensor ->
                            message.reply(
                              Sensor.asJson(savedSensor)
                            )
                        )
                        .onFailure(throwable -> message.fail(500, "Unable to save sensor"));
                      break;
                    default:
                      message.fail(400, "Unknown action.");
                  }
                });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    log.debug("Un-deploying sensor-service");
    consumer.unregister(stopPromise);
  }
}
