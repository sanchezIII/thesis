package org.traffic.traffic_producer.experimental.data;

import io.vertx.core.json.JsonObject;
import org.traffic.traffic_producer.experimental.core.ObservationData;

public class DoubleObservationData implements ObservationData {
  private Double value;

  @Override
  public boolean equals(ObservationData other) {
    if( !(other.getValue() instanceof Double) )
      return false;
    return value.equals((Double)other.getValue());
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public Object getJsonRepresentation() {
    return value;
  }
}
