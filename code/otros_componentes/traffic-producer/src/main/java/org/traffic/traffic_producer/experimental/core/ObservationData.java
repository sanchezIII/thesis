package org.traffic.traffic_producer.experimental.core;

import io.vertx.core.json.JsonObject;

public interface ObservationData<T> {

  public boolean equals(ObservationData other);

  public T getValue();


//  Esto puede ser un json object o puede ser un valor Number o String.
  public Object getJsonRepresentation();

}
