package cu.uclv.mfc.enigma.streams;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.util.JsonCollector;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public final class StreamService extends AbstractVerticle {

  private final Log log = new Log(StreamService.class);
  public static final String STREAM_SERVICE_ADDRESS = "stream-service";
  private StreamRepository repository;
  private MessageConsumer<JsonObject> consumer;

  public StreamService(StreamRepository repository) {
    this.repository = repository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
    if (repository == null) repository =
      new StreamMongoRepository(vertx, context.config());
  }

  @Override
  public void start() {
    consumer =
      vertx
        .eventBus()
        .consumer(
          STREAM_SERVICE_ADDRESS,
          message -> {
            switch (message.headers().get("action")) {
              case "saveAll":
                val streams = message
                  .body()
                  .getJsonArray("list")
                  .stream()
                  .map(__ -> (JsonObject) __)
                  .map(Stream::fromJson)
                  .collect(Collectors.toList());
                repository
                  .saveAll(streams)
                  .onSuccess(streamList -> {
                    message.reply(
                      new JsonObject()
                        .put("list", message.body().getJsonArray("list"))
                    );
                  })
                  .onFailure(throwable -> {
                    message.fail(500, "Unable to save streams");
                  });
                break;
              case "save":
                val stream = Stream.fromJson(message.body());
                repository
                  .save(stream)
                  .onSuccess(saved -> message.reply(message.body()));
                break;
              case "findOne":
                val sensorId = message.body().getString("sensorId");
                val feature = message.body().getString("feature");
                repository
                  .find(sensorId, feature)
                  .onSuccess(hit -> message.reply(Stream.asJson(hit)))
                  .onFailure(throwable -> message.fail(404, "Stream not found")
                  );
                break;
              case "findAll":
                repository
                  .findAll()
                  .onSuccess(streamList -> {
                    val payload = new JsonObject()
                      .put(
                        "list",
                        streamList
                          .stream()
                          .map(Stream::asJson)
                          .collect(Collectors.toList())
                      );
                    message.reply(payload);
                  })
                  .onFailure(throwable ->
                    message.fail(500, "Unable to fetch all streams")
                  );
                break;
              case "deleteAllBy":
                val sensors = message
                  .body()
                  .getJsonArray("list")
                  .stream()
                  .map(__ -> ((String) __))
                  .collect(Collectors.toList());
                //                          log.debug(
                //                            "Request to delete stream with ids in: [{}]",
                //                            message.body().getJsonArray("list").encodePrettily());
                repository
                  .deleteAllBy(sensors)
                  .onSuccess(deletedStreams -> {
                    val payload = new JsonObject()
                      .put(
                        "list",
                        deletedStreams
                          .stream()
                          .map(Stream::asJson)
                          .collect(Collectors.toList())
                      );
                    message.reply(payload);
                  })
                  .onFailure(throwable -> {
                    message.fail(500, "Unable to delete requested streams");
                  });
                break;
              case "find":
                String streamId = message.body().getString("value");

                repository
                  .find(new JsonObject().put("id", streamId))
                  .onSuccess(foundStreams -> {
                    JsonArray jsonArray = foundStreams
                      .stream()
                      .map(st -> Stream.asJson(st))
                      .collect(JsonCollector.toJsonArray());

                    message.reply(new JsonObject().put("value", jsonArray));
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
    log.info("Un-deploying stream-service");
    consumer.unregister(stopPromise);
  }
}
