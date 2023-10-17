package org.traffic.traffic_registry.stream;

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
import org.traffic.traffic_registry.point.PointRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.traffic.traffic_registry.stream.StreamService.STREAM_SERVICE_ADDRESS;

@ExtendWith(VertxExtension.class)
@Slf4j
class StreamServiceTest {

  private static Stream stream;

  private static final String __ = "Foo";

  @BeforeAll
  static void prepare(Vertx vertx) {
    stream =
        Stream.fromJson(new JsonObject(vertx.fileSystem().readFileBlocking("streams/stream.json")));
  }

  @Nested
  class WithSucceedingRepository {

    @BeforeEach
    void deploy(Vertx vertx, VertxTestContext testContext) {
      val streamRepositoryMock = mock(StreamRepository.class);
      when(streamRepositoryMock.save(any())).thenReturn(Future.succeededFuture(__));

      val pointRepositoryMock = mock(PointRepository.class);
      when(pointRepositoryMock.save(any())).thenReturn(Future.succeededFuture(__));

      val verticle = new StreamService(streamRepositoryMock, pointRepositoryMock);
      vertx
          .deployVerticle(verticle, new DeploymentOptions().setWorker(true))
          .onComplete(testContext.succeedingThenComplete());
    }

    @Test
    void onSaveTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
      val options = new DeliveryOptions().addHeader("action", "save");
      vertx
          .eventBus()
          .<JsonObject>request(STREAM_SERVICE_ADDRESS, Stream.asJson(stream), options)
          .onComplete(
              testContext.succeeding(
                  rdf ->
                      testContext.verify(
                          () -> {
                            Assertions.assertEquals(__, rdf.body().getString("result"));
                            testContext.completeNow();
                          })));

      assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

      if (testContext.failed()) {
        throw testContext.causeOfFailure();
      }
    }
  }
}
