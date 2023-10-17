package org.traffic.traffic_registry.sensor;

import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.StringReader;

@Slf4j
@ExtendWith(VertxExtension.class)
class StardogRDF4JSensorRepositoryTest {

  private static JsonObject config;

  private static Sensor sensor;

  private static String rdfSensor;

  @BeforeAll
  static void configure(Vertx vertx) {
    config = new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sensor-repository.json"));
    sensor =
        Sensor.fromJson(new JsonObject(vertx.fileSystem().readFileBlocking("sensors/sensor.json")));
    rdfSensor = vertx.fileSystem().readFileBlocking("sensors/sensor.ttl").toString();
    val databaseName = config.getString("database-name");
    val server = config.getString("server");
    val username = config.getString("username");
    val password = config.getString("password");

    try (val adminConnection =
        AdminConnectionConfiguration.toServer(server).credentials(username, password).connect()) {
      if (adminConnection.list().contains(databaseName)) {
        adminConnection.drop(databaseName);
      }
    }
  }

  @Test
  @DisplayName("When a sensor is saved then the returned RDF format must match")
  void saveTest(VertxTestContext testContext) {
    val repository = new StardogRDF4JSensorRepository(config);
    val result = repository.save(sensor);
    result.onComplete(
        testContext.succeeding(
            rdf ->
                testContext.verify(
                    () -> {
                      val given = Rio.parse(new StringReader(rdf), RDFFormat.TURTLE);
                      val expected = Rio.parse(new StringReader(rdfSensor), RDFFormat.TURTLE);
                      Assertions.assertEquals(expected, given);
                      testContext.completeNow();
                    })));
  }
}
