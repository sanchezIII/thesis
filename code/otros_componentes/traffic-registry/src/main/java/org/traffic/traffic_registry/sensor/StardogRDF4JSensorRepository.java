package org.traffic.traffic_registry.sensor;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.traffic.traffic_registry.Vocabulary;
import org.traffic.traffic_registry.common.AbstractStardogRDFRepository;
import org.traffic.traffic_registry.common.exceptions.ConflictException;
import org.traffic.traffic_registry.common.exceptions.NotFoundException;

import java.io.StringWriter;

import static org.traffic.traffic_registry.Vocabulary.*;
import static org.traffic.traffic_registry.sensor.SensorRepository.toLocalName;

@Slf4j
public final class StardogRDF4JSensorRepository extends AbstractStardogRDFRepository
    implements SensorRepository {

  public StardogRDF4JSensorRepository(JsonObject config) {
    super(config);
  }

  @Override
  public Future<String> save(Sensor sensor) {

    val iri = Values.iri(namespace, toLocalName(sensor.getId()));

    try (val connection = repository.getConnection()) {
      try (val statements = connection.getStatements(iri, null, null)) {
        if (statements.hasNext()) {
          log.debug("Sensor: [{}] already existed", sensor.getId());
          return Future.failedFuture(new ConflictException());
        } else {
          log.debug("Saving new sensor");
          val model =
              new ModelBuilder()
                  .setNamespace(SOSA)
                  .setNamespace(IOT_LITE)
                  .setNamespace(QU)
                  .subject(iri)
                  .add(RDF.TYPE, SENSOR)
                  .add(HAS_QUANTITY_KIND, sensor.getQuantityKind())
                  .add(HAS_UNIT, sensor.getUnit())
                  .build();
          connection.begin();
          connection.add(model);
          connection.commit();
          val rdf = new StringWriter();
          Rio.write(model, rdf, RDFFormat.TURTLE);
          return Future.succeededFuture(rdf.toString());
        }
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<String> findById(String id) {
    val iri = Values.iri(namespace, toLocalName(id));
    try (val connection = repository.getConnection()) {
      try (val statements = connection.getStatements(iri, null, null)) {
        if (statements.hasNext()) {
          val model = QueryResults.asModel(statements);
          model.setNamespace(namespace);
          model.setNamespace(SOSA);
          model.setNamespace(IOT_LITE);
          model.setNamespace(QU);
          val writer = new StringWriter();
          Rio.write(model, writer, RDFFormat.TURTLE);
          return Future.succeededFuture(writer.toString());
        } else {
          return Future.failedFuture(new NotFoundException());
        }
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<String> findAll() {
    try (val connection = repository.getConnection()) {
      val queryString =
          String.format(
              "PREFIX sosa: <%s> \n"
                  + "PREFIX iot-lite: <%s> \n"
                  + "PREFIX qu: <%s> \n"
                  + "CONSTRUCT { ?sensor rdf:type sosa:sensor; \n"
                  + " iot-lite:hasUnit ?unit; \n"
                  + " iot-lite:hasQuantityKind ?quantityKind. \n } "
                  + "WHERE { ?sensor rdf:type sosa:sensor; \n"
                  + " iot-lite:hasUnit ?unit; \n"
                  + " iot-lite:hasQuantityKind ?quantityKind. \n } ",
              Vocabulary.SOSA.getName(), Vocabulary.IOT_LITE.getName(), Vocabulary.QU.getName());

      val graphQuery = connection.prepareGraphQuery(queryString);
      try (val queryResult = graphQuery.evaluate()) {
        val model = QueryResults.asModel(queryResult);
        model.setNamespace(Vocabulary.SOSA);
        model.setNamespace(Vocabulary.IOT_LITE);
        model.setNamespace(Vocabulary.QU);
        val writer = new StringWriter();
        Rio.write(model, writer, RDFFormat.TURTLE);
        return Future.succeededFuture(writer.toString());
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public void shutdown() {
    repository.shutDown();
  }
}
