package org.traffic.traffic_registry.stream;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.traffic.traffic_registry.common.AbstractStardogRDFRepository;
import org.traffic.traffic_registry.common.exceptions.ConflictException;
import org.traffic.traffic_registry.common.exceptions.NotFoundException;
import org.traffic.traffic_registry.point.PointRepository;
import org.traffic.traffic_registry.sensor.SensorRepository;

import java.io.StringWriter;

import static org.traffic.traffic_registry.Vocabulary.*;
import static org.traffic.traffic_registry.stream.StreamRepository.toLocalName;

@Slf4j
public final class StardogRDF4JStreamRepository extends AbstractStardogRDFRepository
    implements StreamRepository {

  public StardogRDF4JStreamRepository(JsonObject config) {
    super(config);
  }

  @Override
  public Future<String> save(Stream stream) {
    try (val connection = repository.getConnection()) {
      val streamIri = Values.iri(namespace, toLocalName(stream.getId()));
      val streamPointIri =
          Values.iri(namespace, PointRepository.toLocalName(stream.getLocation().getId()));
      val sensorIri = Values.iri(namespace, SensorRepository.toLocalName(stream.getGeneratedBy()));
      try (val statements = connection.getStatements(streamIri, null, null)) {
        val writer = new StringWriter();
        // Stream does not exist
        if (!statements.hasNext()) {
          log.info("Saving stream: [{}]", stream);
          connection.begin();
          val modelBuilder =
              new ModelBuilder()
                  .setNamespace(namespace)
                  .setNamespace(IOT_STREAM)
                  .setNamespace(GEO)
                  .subject(streamIri)
                  .add(RDF.TYPE, STREAM)
                  .add(LOCATION, streamPointIri)
                  .add(STREAM_START, stream.getStreamStart())
                  .add(GENERATED_BY, sensorIri);

          if (stream.getDerivedFrom() != null)
            modelBuilder.add(DERIVED_FROM, stream.getDerivedFrom());

          if (stream.getStreamEnd() != null) {
            modelBuilder.add(STREAM_END, stream.getStreamEnd());
          }

          val model = modelBuilder.build();
          connection.add(model);
          connection.commit();
          Rio.write(model, writer, RDFFormat.TURTLE);
        } else {
          log.debug("Stream: [{}] already existed", stream.getId());
          return Future.failedFuture(new ConflictException());
        }
        return Future.succeededFuture(writer.toString());
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<String> findById(String id) {
    try (val connection = repository.getConnection()) {
      val iri = Values.iri(namespace, toLocalName(id));
      try (val statements = connection.getStatements(iri, null, null)) {
        if (statements.hasNext()) {
          val model = QueryResults.asModel(statements);
          model.setNamespace(namespace);
          model.setNamespace(IOT_STREAM);
          model.setNamespace(GEO);
          val rdfWriter = new StringWriter();
          Rio.write(model, rdfWriter, RDFFormat.TURTLE);
          return Future.succeededFuture(rdfWriter.toString());
        } else {
          return Future.failedFuture(new NotFoundException());
        }
      }
    }
  }

  @Override
  public Future<String> findAll() {
    try (val connection = repository.getConnection()) {
      val queryString =
          String.format(
              "PREFIX iot-stream: <%s> \n"
                  + "PREFIX geo: <%s> \n"
                  + "PREFIX xmlschema: <%s>\n"
                  + "CONSTRUCT {\n"
                  + "    ?sensor rdf:type iot-stream:IotStream;\n"
                  + "            iot-stream:windowStart ?streamStart;\n"
                  + "            iot-stream:windowEnd ?windowEnd;\n"
                  + "            iot-stream:generatedBy ?generatedBy;\n"
                  + "            geo:location ?point.\n"
                  + "    ?point rdf:type geo:Point;\n"
                  + "           geo:lat ?lat;\n"
                  + "           geo:long ?long.\n"
                  + "} WHERE {\n"
                  + "        ?sensor rdf:type iot-stream:IotStream;\n"
                  + "            iot-stream:windowStart ?streamStart. \n"
                  + "            OPTIONAL { ?sensor iot-stream:windowEnd ?windowEnd }\n."
                  + "            ?sensor iot-stream:generatedBy ?generatedBy;\n"
                  + "            geo:location ?point.\n"
                  + "        ?point rdf:type geo:Point;\n"
                  + "            geo:lat ?lat;\n"
                  + "            geo:long ?long.\n"
                  + "}",
              IOT_STREAM.getName(), GEO.getName(), XSD.DATETIME.getLocalName());

      val tupleQuery = connection.prepareGraphQuery(queryString);
      //      System.out.println(queryString);
      try (val result = tupleQuery.evaluate()) {
        val model = QueryResults.asModel(result);
        val writer = new StringWriter();
        model.setNamespace(IOT_STREAM);
        model.setNamespace(GEO);
        model.setNamespace(Values.namespace(XSD.PREFIX, XSD.NAMESPACE));
        Rio.write(model, writer, RDFFormat.TURTLE);
        return Future.succeededFuture(writer.toString());
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }
}
