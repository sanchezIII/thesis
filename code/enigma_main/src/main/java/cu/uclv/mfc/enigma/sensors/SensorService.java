package cu.uclv.mfc.enigma.sensors;

import static cu.uclv.mfc.enigma.topics.TopicService.TOPIC_SERVICE_ADDRESS;
import static cu.uclv.mfc.enigma.util.ObjectBuilder.action;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.util.JsonCollector;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public final class SensorService extends AbstractVerticle {

  public static final String SENSOR_SERVICE_ADDRESS = "sensor-service";
  private final Log log = new Log(SensorService.class);
  private SensorRepository repository;
  private MessageConsumer<JsonObject> consumer;

  public SensorService(SensorRepository repository) {
    this.repository = repository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository =
      new SensorMongoRepository(vertx, context.config());
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
              case "saveAll":
                val sensors = message
                  .body()
                  .getJsonArray("list")
                  .stream()
                  .map(__ -> (JsonObject) __)
                  .map(Sensor::fromJson)
                  .collect(Collectors.toList());

                repository
                  .saveAll(sensors)
                  .onSuccess(savedSensors ->
                    message.reply(
                      new JsonObject()
                        .put("list", message.body().getJsonArray("list"))
                    )
                  )
                  .onFailure(throwable ->
                    message.fail(500, "Unable to save sensors")
                  );
              case "save":
                Sensor sensor = Sensor.fromJson(message.body());

                checkTopicExistence(sensor.getTopic())
                  .compose(result -> {
                    if (result) {
                      return repository.save(sensor);
                    } else {
                      log.sys(
                        "Refuse saving sensor {}. No such topic in the system.",
                        sensor
                      );
                      return Future.failedFuture(
                        "No such topic in the system: " + sensor.getTopic()
                      );
                    }
                  })
                  .onSuccess(savedSensor ->
                    message.reply(Sensor.asJson(savedSensor))
                  )
                  .onFailure(throwable ->
                    message.fail(500, throwable.getMessage())
                  );
                break;
              case "getByTopicId":
                String topicId = message.body().getString("topicId");
                JsonObject query = new JsonObject().put("topic", topicId);

                repository
                  .find(query)
                  .compose(sensorList ->
                    Future.succeededFuture(
                      sensorList
                        .stream()
                        .map(Sensor::asJson)
                        .collect(Collectors.toList())
                    )
                  )
                  .compose(jsonList ->
                    Future.succeededFuture(
                      new JsonObject()
                        .put(
                          "data",
                          jsonList.stream().collect(JsonCollector.toJsonArray())
                        )
                    )
                  )
                  .onSuccess(message::reply)
                  .onFailure(throwable ->
                    message.fail(
                      500,
                      "Unable to retrieve sensor list from repo"
                    )
                  );
                break;
              case "find":
                repository
                  .find(message.body())
                  .map(sensorList ->
                    new JsonObject()
                      .put(
                        "value",
                        sensorList
                          .stream()
                          .map(Sensor::asJson)
                          .collect(JsonCollector.toJsonArray())
                      )
                  )
                  .onSuccess(message::reply)
                  .onFailure(throwable ->
                    message.fail(
                      500,
                      "Unable to retrieve sensor list from repo"
                    )
                  );
                break;
              default:
                message.fail(400, "Unknown action.");
            }
          }
        );
  }

  private Future<Boolean> checkTopicExistence(String topicId) {
    return vertx
      .eventBus()
      .request(
        TOPIC_SERVICE_ADDRESS,
        new JsonObject().put("value", topicId),
        action("count")
      )
      .compose(reply -> {
        JsonObject json = (JsonObject) reply.body();

        return Future.succeededFuture(json.getLong("value") > 0);
      });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    System.out.println("Un-deploying sensor-service");
    consumer.unregister(stopPromise);
  }
}
