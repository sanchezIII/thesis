package cu.uclv.mfc.enigma.topics;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.util.JsonCollector;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class TopicService extends AbstractVerticle {

  private final Log log = new Log(TopicService.class);

  private TopicCollection topics;

  public static final String TOPIC_SERVICE_ADDRESS = "topic-service";

  private TopicRepository repository;

  public TopicService(TopicCollection topics) {
    this.topics = topics;
  }

  public TopicService(TopicCollection topics, TopicRepository repository) {
    this.topics = topics;

    this.repository = new TopicMongoRepository(vertx, context.config());
  }

  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository =
      new TopicMongoRepository(vertx, context.config());
  }

  @Override
  public void start() throws Exception {
    /* Load Topics from Mongo Repository */
    repository
      .find(new JsonObject())
      .onSuccess(topicList -> {
        for (Topic topic : topicList) topics.add(topic);

        log.info("Loaded {} topics from database.", topics.size());
      })
      .onFailure(throwable -> {
        log.error(
          "Error loading topics from database. Cause: {}.",
          throwable.getMessage()
        );
      });

    MessageConsumer<JsonObject> consumer = vertx
      .eventBus()
      .consumer(
        TOPIC_SERVICE_ADDRESS,
        message -> {
          String topicId;

          switch (message.headers().get("action")) {
            case "create":
              try {
                Topic topic = Topic.fromJson(message.body());
                //                              TODO: Hay un error: no se dice nada cuando el try falla

                topics.add(topic);

                repository
                  .save(topic)
                  .onSuccess(savedTopic -> message.reply(Topic.asJson(topic)))
                  .onFailure(throwable -> {
                    message.fail(500, "Unable to save topic");
                  });
              } catch (ClassNotFoundException e) {
                message.fail(400, "Unknown class from JSON representation.");
              }
              break;
            case "find":
              topicId = message.body().getString("value");

              repository
                .find(new JsonObject().put("id", topicId))
                .onSuccess(foundTopics -> {
                  JsonArray jsonArray = foundTopics
                    .stream()
                    .map(topic -> Topic.asJson(topic))
                    .collect(JsonCollector.toJsonArray());

                  message.reply(new JsonObject().put("value", jsonArray));
                })
                .onFailure(throwable -> {
                  //                              log.info(throwable.toString());
                  message.fail(500, throwable.getMessage());
                });
              break;
            case "count":
              topicId = message.body().getString("value");

              repository
                .count(new JsonObject().put("id", topicId))
                .onSuccess(cant -> {
                  message.reply(new JsonObject().put("value", cant));
                })
                .onFailure(throwable ->
                  message.fail(500, throwable.getMessage())
                );

              break;
            default:
              message.fail(400, "Unknown action.");
          }
        }
      );
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    //        System.out.println("Un-deploying sensor-service");
    //        consumer.unregister(stopPromise);
  }
}
