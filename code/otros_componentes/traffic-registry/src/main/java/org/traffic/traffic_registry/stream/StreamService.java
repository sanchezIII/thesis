package org.traffic.traffic_registry.stream;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_registry.common.exceptions.ConflictException;
import org.traffic.traffic_registry.common.exceptions.NotFoundException;
import org.traffic.traffic_registry.point.PointRepository;
import org.traffic.traffic_registry.point.StardogRDF4JPointRepository;

import static java.lang.String.format;

@Slf4j
@NoArgsConstructor
public final class StreamService extends AbstractVerticle {

  public static final String STREAM_SERVICE_ADDRESS = "registry.stream-service";

  private StreamRepository streamRepository;

  private PointRepository pointRepository;

  private MessageConsumer<JsonObject> consumer;

  public StreamService(StreamRepository streamRepository, PointRepository pointRepository) {
    this.streamRepository = streamRepository;
    this.pointRepository = pointRepository;
  }

  @Override
  public void init(Vertx vertx, Context context) {

    super.init(vertx, context);
    if (streamRepository == null)
      streamRepository =
          new StardogRDF4JStreamRepository(context.config().getJsonObject("stream-repository"));

    if (pointRepository == null)
      pointRepository =
          new StardogRDF4JPointRepository(context.config().getJsonObject("point-repository"));
  }

  @Override
  public void start() {
    consumer =
        vertx
            .eventBus()
            .consumer(
                STREAM_SERVICE_ADDRESS,
                message -> {
                  val action = message.headers().get("action");
                  switch (action) {
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
    streamRepository
        .findAll()
        .onSuccess(
            streamGraph -> {
              message.reply(new JsonObject().put("result", streamGraph));
            })
        .onFailure(throwable -> message.fail(500, throwable.toString()));
  }

  private void find(Message<JsonObject> message) {
    val id = message.body().getString("id");
    streamRepository
        .findById(id)
        .onSuccess(
            rdf -> {
              log.info("Found stream with id: {}", id);
              message.reply(new JsonObject().put("result", rdf));
            })
        .onFailure(
            throwable -> {
              if (throwable instanceof NotFoundException)
                message.fail(404, String.format("Stream not found with id: %s", id));
              else message.fail(500, throwable.getMessage());
            });
  }

  private void save(Message<JsonObject> message) {
    val stream = Stream.fromJson(message.body());
    pointRepository
        .save(stream.getLocation())
        .recover(__ -> Future.succeededFuture())
        .compose(
            __ -> streamRepository.save(stream))
        .onSuccess(
            streamGraph -> {
              log.info("Successfully inserted stream: {}", stream.getId());
              message.reply(new JsonObject().put("result", streamGraph));
            })
        .onFailure(
            throwable -> {
              if (throwable instanceof ConflictException)
                message.fail(409, "Stream already existed");
              else message.fail(500, format("Unable to save stream: [%s]", stream.getId()));
            });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.unregister(stopPromise);
  }
}
