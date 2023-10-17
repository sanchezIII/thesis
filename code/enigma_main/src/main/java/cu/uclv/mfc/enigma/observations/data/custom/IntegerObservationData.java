package cu.uclv.mfc.enigma.observations.data.custom;

import cu.uclv.mfc.enigma.observations.data.ObservationData;
import io.vertx.core.json.JsonObject;

public class IntegerObservationData implements ObservationData {

  private Integer value;

  @Override
  public boolean equals(ObservationData other) {
    if (!(other.getValue() instanceof Integer)) return false;
    return getValue().equals((Integer) other.getValue());
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setValue(Object value) {
    this.value = (Integer) value;
  }

  @Override
  public JsonObject getJsonRepresentation() {
    return new JsonObject().put("value", value);
  }

  @Override
  public ObservationData getObservationDataFromJson(JsonObject json) {
    ObservationData data = new IntegerObservationData();
    data.setValue(json.getInteger("value"));
    return data;
  }
}
