package org.traffic.traffic_producer.observations;

import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
public final class ObservationService extends AbstractVerticle {

  public static final String OBSERVATION_SERVICE_ADDRESS = "observation-service";
  private ObservationRepository repository;
  private MessageConsumer<JsonObject> consumer;

  public ObservationService(ObservationRepository repository) {
    this.repository = repository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository = new ObservationRedisRepository(vertx, context.config());
  }

  @Override
  public void start() {
    consumer =
        vertx
            .eventBus()
            .consumer(
                OBSERVATION_SERVICE_ADDRESS,
                message -> {
                  switch (message.headers().get("action")) {
                    case "publish":
                      repository
                          .publish(Observation.fromJson(message.body()))
                          .onSuccess(observation -> message.reply(Observation.asJson(observation)))
                          .onFailure(
                              throwable ->
                                  message.fail(
                                      500,
                                      String.format(
                                          "Unable to publish observation: %s",
                                          message.body().toString())));
                      break;
                    case "publishAll":
                      val observations =
                          message.body().getJsonArray("list").stream()
                              .map(__ -> ((JsonObject) __))
                              .map(Observation::fromJson)
                              .collect(Collectors.toList());
                      val all =
                          CompositeFuture.all(
                              observations.stream()
                                  .map(repository::publish)
                                  .collect(Collectors.toList()));
                      all.onSuccess(
                          __ -> {
                            log.info("Observation batch has been sent");
                            message.reply(message.body());
                          });
                      all.onFailure(
                          throwable -> {
                            message.fail(500, "Unable to send observations");
                            log.error("Unable to send observation batch");
                            throwable.printStackTrace();
                          });
                      break;
                    default:
                      log.error("Action is: {}", message.headers().get("action"));
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
