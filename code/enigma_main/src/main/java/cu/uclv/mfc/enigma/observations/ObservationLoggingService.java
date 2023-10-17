package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicNotFoundException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

public class ObservationLoggingService extends AbstractVerticle {

  public static final String OBSERVATION_LOGGING_SERVICE_ADDRESS =
    "observation-logging-service";

  private ObservationRepository repository;

  private MessageConsumer<JsonObject> consumer;

  private TopicCollection topics;

  public ObservationLoggingService(TopicCollection topics) {
    this.topics = topics;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);

    if (repository == null) repository =
      new ObservationMongoRepository(vertx, context.config(), this.topics);
  }

  @Override
  public void start() throws Exception {
    super.start();

    consumer =
      vertx
        .eventBus()
        .consumer(
          OBSERVATION_LOGGING_SERVICE_ADDRESS,
          message -> {
            String topicId = message.headers().get("topic");

            try {
              ObservationDataFactory factory = topics
                .getTopicById(topicId)
                .getFactory();

              switch (message.headers().get("action")) {
                case "save":
                  Observation obs = Observation.fromJson(
                    message.body(),
                    factory
                  );

                  repository
                    .publish(obs, topicId)
                    .onSuccess(savedSensor ->
                      message.reply(Observation.asJson(savedSensor, factory))
                    )
                    .onFailure(throwable ->
                      message.fail(500, "Unable to save observation")
                    );
                  break;
                default:
                  message.fail(400, "Unknown action.");
              }
            } catch (TopicNotFoundException e) {
              message.fail(400, "Topic Not Found.");
            }
          }
        );
  }
}
