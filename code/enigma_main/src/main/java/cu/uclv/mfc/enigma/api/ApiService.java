package cu.uclv.mfc.enigma.api;

import static cu.uclv.mfc.enigma.observations.ObservationService.OBSERVATION_SERVICE_ADDRESS;

import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.observations.ObservationLoggingService;
import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import cu.uclv.mfc.enigma.sensors.Sensor;
import cu.uclv.mfc.enigma.sensors.SensorService;
import cu.uclv.mfc.enigma.streams.Stream;
import cu.uclv.mfc.enigma.streams.StreamService;
import cu.uclv.mfc.enigma.topics.Topic;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicNotFoundException;
import cu.uclv.mfc.enigma.topics.TopicService;
import cu.uclv.mfc.enigma.util.JsonCollector;
import cu.uclv.mfc.enigma.util.ObjectBuilder;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.val;

public final class ApiService extends AbstractVerticle {

  private static final Log log = new Log(ApiService.class);

  private final TopicCollection topics;
  private JsonObject config;

  public ApiService(TopicCollection topics) {
    this.topics = topics;
  }

  private static void failureHandler(RoutingContext routingContext) {
    Throwable failure = routingContext.failure();

    log.error("Request failed: " + failure.getMessage());

    routingContext
      .response()
      .setStatusCode(500)
      .setStatusMessage("Server internal error:" + failure.getMessage())
      .end();
  }

  public Router createRouter() {
    Router router = Router.router(vertx);

    router
      .route("/")
      .handler(routingContext -> {
        HttpServerResponse response = routingContext.response();

        log.info("Request to /");

        response
          .putHeader("content-type", "text/html")
          .end("Enigma API Running!!!");
      });

    /* ADD SENSOR METHOD */
    router.route(Constants.addSensorRoute + "*").handler(BodyHandler.create());
    router
      .route(Constants.addSensorRoute)
      .handler(routingContext -> {
        JsonObject json = routingContext.body().asJsonObject();

        Sensor sensor = Sensor.fromJson(json);
        String topicId = json.getString("topic");

        vertx
          .eventBus()
          .request(
            SensorService.SENSOR_SERVICE_ADDRESS,
            Sensor.asJson(sensor),
            ObjectBuilder.action("save").addHeader("topic", topicId)
          )
          .onSuccess(reply -> {
            Sensor savedSensor = Sensor.fromJson((JsonObject) reply.body());

            routingContext
              .response()
              .setStatusCode(201)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildOKApiResponse(Sensor.asJson(savedSensor)).toString());
          })
          .onFailure(fail -> {
            ///////

            routingContext
              .response()
              .setStatusCode(500)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildFailedApiResponse(fail).toString());
          });
      })
      .failureHandler(ApiService::failureHandler);

    /* ADD STREAM METHOD */
    router.route(Constants.addStreamRoute + "*").handler(BodyHandler.create());
    router
      .route(Constants.addStreamRoute)
      .handler(routingContext -> {
        JsonObject json = routingContext.body().asJsonObject();
        Stream stream = Stream.fromJson(json);

        vertx
          .eventBus()
          .request(
            StreamService.STREAM_SERVICE_ADDRESS,
            Stream.asJson(stream),
            ObjectBuilder.action("save")
          )
          .onSuccess(reply -> {
            Stream savedStream = Stream.fromJson((JsonObject) reply.body());

            routingContext
              .response()
              .setStatusCode(201)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildOKApiResponse(Stream.asJson(savedStream)).toString());
          })
          .onFailure(fail -> {
            routingContext
              .response()
              .setStatusCode(500)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildFailedApiResponse(fail).toString());
          });
      })
      .failureHandler(ApiService::failureHandler);

    /* ADD OBSERVATION METHOD */
    router
      .route(Constants.addObservationRoute + "*")
      .handler(BodyHandler.create());
    router
      .route(Constants.addObservationRoute)
      .handler(routingContext -> {
        JsonObject json = routingContext.body().asJsonObject();
        String streamId = json.getString("streamId");

        getObservationTopic(streamId)
          .compose(topic -> {
            ObservationDataFactory factory = null;

            try {
              factory = topics.getTopicById(topic.getId()).getFactory();
            } catch (TopicNotFoundException e) {
              return Future.failedFuture(e.getMessage());
            }

            Observation observation = Observation.fromJson(json, factory);

            Future<Message<JsonObject>> publishToRedis;

            if (config.getBoolean("start-observation-service")) publishToRedis =
              vertx
                .eventBus()
                .request(
                  OBSERVATION_SERVICE_ADDRESS,
                  Observation.asJson(observation, factory),
                  ObjectBuilder.action("publish")
                ); else publishToRedis = Future.succeededFuture();

            Future<Message<JsonObject>> publishToMongo;

            if (config.getBoolean("start-observation-logging-service")) {
              publishToMongo =
                vertx
                  .eventBus()
                  .request(
                    ObservationLoggingService.OBSERVATION_LOGGING_SERVICE_ADDRESS,
                    Observation.asJson(observation, factory),
                    ObjectBuilder
                      .action("save")
                      .addHeader("topic", topic.getId())
                  );
            } else publishToMongo = Future.succeededFuture();

            return CompositeFuture.all(publishToRedis, publishToMongo);
          })
          .onSuccess(compositeFuture -> {
            val jsonArray = compositeFuture
              .list()
              .stream()
              .filter(message -> message != null)
              .map(message -> (Message<JsonObject>) message)
              .map(message -> message.body())
              .collect(JsonCollector.toJsonArray());

            routingContext
              .response()
              .setStatusCode(201)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildOKApiResponse(jsonArray).toString());
          })
          .onFailure(fail -> {
            routingContext
              .response()
              .setStatusCode(201)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildFailedApiResponse(fail).toString());
          });
      })
      .failureHandler(ApiService::failureHandler);

    /* ADD TOPIC METHOD */
    router.route(Constants.addTopicRoute + "*").handler(BodyHandler.create());
    router
      .route(Constants.addTopicRoute)
      .handler(routingContext -> {
        JsonObject json = routingContext.body().asJsonObject();

        try {
          Topic topic = Topic.fromJson(json);

          vertx
            .eventBus()
            .request(
              TopicService.TOPIC_SERVICE_ADDRESS,
              Topic.asJson(topic),
              ObjectBuilder.action("create")
            )
            .onSuccess(reply -> {
              routingContext
                .response()
                .setStatusCode(201)
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(buildOKApiResponse((JsonObject) reply.body()).toString());
            })
            .onFailure(fail -> {
              routingContext
                .response()
                .setStatusCode(400)
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(buildFailedApiResponse(fail.getCause()).toString());
            });
        } catch (ClassNotFoundException e) {
          routingContext
            .response()
            .setStatusCode(400)
            .putHeader("content-type", "application/text; charset=utf-8")
            .end(buildFailedApiResponse(e).toString());
        }
      })
      .failureHandler(ApiService::failureHandler);

    /* GET-SENSORS-FROM-TOPIC METHOD */
    router
      .get(Constants.getSensorsByTopicIdRoute)
      .handler(routingContext -> {
        String topicId = routingContext.request().getParam("topic");
        JsonObject query = new JsonObject().put("topicId", topicId);

        vertx
          .eventBus()
          .request(
            SensorService.SENSOR_SERVICE_ADDRESS,
            query,
            ObjectBuilder.action("getByTopicId")
          )
          .onSuccess(replay -> {
            val result = (JsonObject) replay.body();

            routingContext
              .response()
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildOKApiResponse(result.getJsonArray("data")).toString());
          })
          .onFailure(fail -> {
            routingContext
              .response()
              .setStatusCode(400)
              .putHeader("content-type", "application/json; charset=utf-8")
              .end(buildFailedApiResponse(fail.getCause()).toString());
          });
      });

    //        TODO: publish all

    return router;
  }

  private Future<Topic> getObservationTopic(String streamId) {
    return vertx
      .eventBus()
      .request(
        StreamService.STREAM_SERVICE_ADDRESS,
        new JsonObject().put("value", streamId),
        ObjectBuilder.action("find")
      )
      .compose(message -> {
        val result = (JsonObject) message.body();
        val jsonArray = result.getJsonArray("value");

        if (jsonArray.size() == 0) return Future.failedFuture(
          "No such stream: " + streamId
        ); else if (jsonArray.size() > 1) log.warn(
          "Existen varios streams con el mismo id: " + streamId
        );

        return Future.succeededFuture(
          Stream.fromJson(jsonArray.getJsonObject(0))
        );
      })
      .map(stream -> stream.getGeneratedBy())
      .compose(sensorId ->
        vertx
          .eventBus()
          .request(
            SensorService.SENSOR_SERVICE_ADDRESS,
            new JsonObject().put("id", sensorId),
            ObjectBuilder.action("find")
          )
          .compose(message -> {
            val result = (JsonObject) message.body();
            val jsonArray = result.getJsonArray("value");

            if (jsonArray.size() == 0) return Future.failedFuture(
              "No such sensor: " + sensorId
            ); else if (jsonArray.size() > 1) log.warn(
              "Existen varios sensores con el mismo id: " + sensorId
            );

            return Future.succeededFuture(
              Sensor.fromJson(jsonArray.getJsonObject(0))
            );
          })
      )
      .compose(sensor ->
        vertx
          .eventBus()
          .request(
            TopicService.TOPIC_SERVICE_ADDRESS,
            new JsonObject().put("value", sensor.getTopic()),
            ObjectBuilder.action("find")
          )
          .compose(reply -> {
            JsonObject json = (JsonObject) reply.body();
            JsonArray jsonArray = json.getJsonArray("value");

            if (jsonArray.size() == 0) return Future.failedFuture(
              "No such topic: " + sensor.getTopic()
            ); else if (jsonArray.size() > 1) log.warn(
              "Existen varios sensores con el mismo id: " + sensor.getTopic()
            );

            try {
              return Future.succeededFuture(
                Topic.fromJson(jsonArray.getJsonObject(0))
              );
            } catch (ClassNotFoundException e) {
              return Future.failedFuture(e.getMessage());
            }
          })
      );
  }

  private JsonObject buildOKApiResponse(JsonObject json) {
    return new JsonObject().put("status", "OK").put("data", json);
  }

  private JsonObject buildFailedApiResponse(Throwable throwable) {
    return new JsonObject()
      .put("status", "FAIL")
      .put("cause", throwable.getMessage());
  }

  private JsonObject buildOKApiResponse(JsonArray json) {
    return new JsonObject().put("status", "OK").put("data", json);
  }

  //    TODO: stop method

  @Override
  public void start(Promise promise) throws Exception {
    ConfigRetriever configRetriever = ConfigRetriever.create(vertx);
    configRetriever
      .getConfig()
      .compose(config -> {
        this.config = config;
        return Future.succeededFuture(config.getJsonObject("api-service"));
      })
      .compose(config -> {
        int port = config.getInteger("port");

        vertx
          .createHttpServer()
          .requestHandler(createRouter())
          .listen(
            config().getInteger("HTTP_PORT", port),
            result -> {
              if (result.succeeded()) {
                promise.complete();
              } else {
                promise.fail(result.cause());
              }
            }
          );
        return Future.succeededFuture(config);
      })
      .onSuccess(config -> {
        log.info(
          "API Started to listen on port {}.",
          config.getInteger("port")
        );
      });
  }
}
