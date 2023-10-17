package org.traffic.traffic_registry.sensor;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_registry.common.exceptions.ConflictException;
import org.traffic.traffic_registry.common.exceptions.NotFoundException;

import static java.lang.String.format;

@Slf4j
@NoArgsConstructor
public final class SensorService extends AbstractVerticle {

  public static final String SENSOR_SERVICE_ADDRESS = "registry.sensor-service";

  private SensorRepository sensorRepository;

  private MessageConsumer<JsonObject> consumer;

  public SensorService(SensorRepository sensorRepository) {
    this.sensorRepository = sensorRepository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (sensorRepository == null)
      sensorRepository =
          new StardogRDF4JSensorRepository(context.config().getJsonObject("sensor-repository"));
  }

  @Override
  public void start() {
    consumer =
        vertx
            .eventBus()
            .consumer(
                SENSOR_SERVICE_ADDRESS,
                message -> {
                  switch (message.headers().get("action")) {
                    case "save":
                      save(message);
                      break;
                    case "find":
                      find(message);
                      break;
                    case "findAll":
                      findAll(message);
                      break;
                    default:
                      message.fail(
                          400, format("Unknown action: [%s]", message.headers().get("action")));
                  }
                });
  }

  private void findAll(Message<JsonObject> message) {
    sensorRepository
        .findAll()
        .onSuccess(
            sensorGraph -> {
              message.reply(new JsonObject().put("result", sensorGraph));
            })
        .onFailure(
            throwable -> {
              message.fail(500, throwable.toString());
            });
  }

  private void find(Message<JsonObject> message) {
    val id = message.body().getString("id");
    sensorRepository
        .findById(id)
        .onSuccess(
            sensorGraph -> {
              log.info("Found sensor with id: {}", id);
              message.reply(new JsonObject().put("result", sensorGraph));
            })
        .onFailure(
            throwable -> {
              if (throwable instanceof NotFoundException) {
                log.debug("Sensor not found: {}", id);
                message.fail(404, "404");
              } else {
                log.debug("Unable to find sensor by id: {}", throwable.toString());
                message.fail(500, throwable.getMessage());
              }
            });
  }

  private void save(Message<JsonObject> message) {
    val sensor = Sensor.fromJson(message.body());
    sensorRepository
        .save(sensor)
        .onSuccess(
            sensorGraph -> {
              log.info("Successfully inserted sensor: {}", sensor);
              message.reply(new JsonObject().put("result", sensorGraph));
            })
        .onFailure(
            throwable -> {
              if (throwable instanceof ConflictException)
                message.fail(409, "Sensor already existed");
              else message.fail(500, format("Unable to save sensor: [%s]", sensor.getId()));
            });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.unregister(stopPromise);
  }
}
