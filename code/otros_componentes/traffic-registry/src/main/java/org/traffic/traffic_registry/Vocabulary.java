package org.traffic.traffic_registry;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.util.Values;

public final class Vocabulary {

  public static final Namespace SOSA = Values.namespace("sosa", "http://www.w3.org/ns/sosa/");

  public static final IRI SENSOR = Values.iri(SOSA, "sensor");

  public static final Namespace QU = Values.namespace("qu", "http://purl.oclc.org/NET/ssnx/qu/qu#");

  public static final Namespace IOT_LITE =
      Values.namespace("iot-lite", "http://purl.oclc.org/NET/UNIS/fiware/iot-lite#");

  public static final IRI HAS_UNIT = Values.iri(IOT_LITE, "hasUnit");

  public static final IRI HAS_QUANTITY_KIND = Values.iri(IOT_LITE, "hasQuantityKind");

  public static final Namespace GEO =
      Values.namespace("wgs84_pos", "https://www.w3.org/2003/01/geo/wgs84_pos/");

  public static final IRI LOCATION = Values.iri(GEO, "location");

  public static final IRI POINT = Values.iri(GEO, "Point");

  public static final IRI LATITUDE = Values.iri(GEO, "lat");

  public static final IRI LONGITUDE = Values.iri(GEO, "long");

  public static final Namespace IOT_STREAM =
      Values.namespace("iot-stream", "http://purl.org/iot/ontology/iot-stream/");

  public static final IRI STREAM = Values.iri(IOT_STREAM, "IotStream");

  public static final IRI STREAM_START = Values.iri(IOT_STREAM, "windowStart");

  public static final IRI STREAM_END = Values.iri(IOT_STREAM, "windowEnd");

  public static final IRI DERIVED_FROM = Values.iri(IOT_STREAM, "derivedFrom");

  public static final IRI ANALYSED_BY = Values.iri(IOT_STREAM, "analysedBy");

  public static final IRI GENERATED_BY = Values.iri(IOT_STREAM, "generatedBy");

  private Vocabulary() {}
}
