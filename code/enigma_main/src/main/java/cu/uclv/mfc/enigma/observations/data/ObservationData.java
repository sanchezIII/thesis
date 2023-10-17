package cu.uclv.mfc.enigma.observations.data;

import io.vertx.core.json.JsonObject;

public interface ObservationData<T> {
  public boolean equals(ObservationData other);

  public T getValue();

  public void setValue(T value);

  public JsonObject getJsonRepresentation();

  public ObservationData getObservationDataFromJson(JsonObject json);
}
