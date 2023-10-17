package cu.uclv.mfc.enigma.observations.data.custom;

import cu.uclv.mfc.enigma.observations.data.ObservationData;
import io.vertx.core.json.JsonObject;

public class EnumObservationData implements ObservationData {

  private String value;

  @Override
  public boolean equals(ObservationData other) {
    if (!(other.getValue() instanceof String)) return false;
    return value.equals((String) other.getValue());
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setValue(Object value) {
    this.value = (String) value;
  }

  @Override
  public JsonObject getJsonRepresentation() {
    return new JsonObject().put("value", value);
  }

  @Override
  public ObservationData getObservationDataFromJson(JsonObject json) {
    ObservationData obsData = new EnumObservationData();
    obsData.setValue(json.getString("value"));
    return obsData;
  }
}
