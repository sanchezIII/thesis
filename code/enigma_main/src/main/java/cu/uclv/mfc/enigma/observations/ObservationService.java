package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicNotFoundException;
import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public final class ObservationService extends AbstractVerticle {

  public static final String OBSERVATION_SERVICE_ADDRESS =
    "observation-service";
  private ObservationRepository repository;
  private MessageConsumer<JsonObject> consumer;

  private TopicCollection topics;
  private final Log log = new Log(ObservationService.class);

  public ObservationService(
    ObservationRepository repository,
    TopicCollection topics
  ) {
    this.repository = repository;
    this.topics = topics;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository =
      new ObservationRedisRepository(vertx, context.config(), topics);
  }

  @Override
  public void start() {
    consumer =
      vertx
        .eventBus()
        .consumer(
          OBSERVATION_SERVICE_ADDRESS,
          message -> {
            String topicId = message.headers().get("topic");

            try {
              ObservationDataFactory factory = topics
                .getTopicById(topicId)
                .getFactory();

              switch (message.headers().get("action")) {
                case "publish":
                  repository
                    .publish(
                      Observation.fromJson(message.body(), factory),
                      topicId
                    )
                    .onSuccess(observation ->
                      message.reply(Observation.asJson(observation, factory))
                    )
                    .onFailure(throwable ->
                      message.fail(
                        500,
                        String.format(
                          "Unable to publish observation: %s",
                          message.body().toString()
                        )
                      )
                    );
                  break;
                case "publishAll":
                  val observations = message
                    .body()
                    .getJsonArray("list")
                    .stream()
                    .map(__ -> ((JsonObject) __))
                    .map(json -> {
                      return Observation.fromJson(json, factory);
                    })
                    .collect(Collectors.toList());

                  val all = CompositeFuture.all(
                    observations
                      .stream()
                      .map(__ -> ((Observation) __))
                      .map(observation -> {
                        return repository.publish(observation, topicId);
                      })
                      .collect(Collectors.toList())
                  );
                  all.onSuccess(__ -> {
                    System.out.println("Observation batch has been sent");
                    message.reply(message.body());
                  });
                  all.onFailure(throwable -> {
                    message.fail(500, "Unable to send observations");
                    log.info("Unable to send observation batch");
                    throwable.printStackTrace();
                  });
                  break;
                default:
                  System.out.printf(
                    "Action is: %s\n",
                    message.headers().get("action").toString()
                  );
                  message.fail(400, "Unknown action.");
              }
            } catch (TopicNotFoundException e) {
              message.fail(400, "Topic not Found.");
            }
          }
        );
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    System.out.println("Un-deploying observation-service");
    consumer.unregister(stopPromise);
  }
}
