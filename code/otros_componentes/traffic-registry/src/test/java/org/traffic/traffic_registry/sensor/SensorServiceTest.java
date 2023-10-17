package org.traffic.traffic_registry.sensor;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.traffic.traffic_registry.sensor.SensorService.SENSOR_SERVICE_ADDRESS;

@Slf4j
@ExtendWith(VertxExtension.class)
class SensorServiceTest {

  private static Sensor sensor;

  private static final String __ = "Foo";

  @BeforeAll
  static void prepare() {
    val vertx = Vertx.vertx();
    sensor =
        Sensor.fromJson(new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sensor.json")));
  }

  @Nested
  @DisplayName("When the service is instantiated with a succeeding repository")
  class WithSucceedingRepository {

    @BeforeEach
    void deploy(Vertx vertx, VertxTestContext testContext) throws Throwable {
      val mock = mock(SensorRepository.class);
      when(mock.save(sensor)).thenReturn(Future.succeededFuture(__));
      vertx
          .deployVerticle(new SensorService(mock), new DeploymentOptions().setWorker(true))
          .onComplete(testContext.succeedingThenComplete());

      Assertions.assertTrue(testContext.awaitCompletion(3, TimeUnit.SECONDS));

      if (testContext.failed()) throw testContext.causeOfFailure();
    }

    @Test
    @DisplayName(
        "When a request to save a sensor is made to the service and the repository "
            + " returns a success then the rdf representation of the sensor must be returned in "
            + " the value associated to the key 'result'")
    void onSaveTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
      val options = new DeliveryOptions().addHeader("action", "save");
      vertx
          .eventBus()
          .<JsonObject>request(SENSOR_SERVICE_ADDRESS, Sensor.asJson(sensor), options)
          .onComplete(
              testContext.succeeding(
                  reply ->
                      testContext.verify(
                          () -> {
                            Assertions.assertTrue(reply.body().containsKey("result"));
                            Assertions.assertEquals(__, reply.body().getString("result"));
                            testContext.completeNow();
                          })));

      assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

      if (testContext.failed()) {
        throw testContext.causeOfFailure();
      }
    }

    @Test
    @DisplayName(
        "When a request is sent to the service with an unknown action then a failure must"
            + " be received")
    void unknownActionTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
      val options = new DeliveryOptions().addHeader("action", "unsupported_action");
      vertx
          .eventBus()
          .<JsonObject>request(SENSOR_SERVICE_ADDRESS, Sensor.asJson(sensor), options)
          .onComplete(testContext.failingThenComplete());

      assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

      if (testContext.failed()) {
        throw testContext.causeOfFailure();
      }
    }
  }

  @Nested
  @DisplayName("When the service is instantiated with a failing repository")
  class WithFailingRepository {

    @BeforeEach
    void deploy(Vertx vertx, VertxTestContext testContext) throws Throwable {
      val mock = mock(SensorRepository.class);
      when(mock.save(any())).thenReturn(Future.failedFuture("Some failure"));
      vertx
          .deployVerticle(new SensorService(mock), new DeploymentOptions().setWorker(true))
          .onComplete(testContext.succeedingThenComplete());

      Assertions.assertTrue(testContext.awaitCompletion(3, TimeUnit.SECONDS));

      if (testContext.failed()) throw testContext.causeOfFailure();
    }

    @Test
    @DisplayName(
        "When a request is made to the service to save a sensor and the repository"
            + " is failing then, a failed future must be returned")
    void saveFailing(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
      val options = new DeliveryOptions().addHeader("action", "save");
      vertx
          .eventBus()
          .<JsonObject>request(SENSOR_SERVICE_ADDRESS, Sensor.asJson(sensor), options)
          .onComplete(testContext.failingThenComplete());

      Assertions.assertTrue(testContext.awaitCompletion(3, TimeUnit.SECONDS));
    }
  }
}
