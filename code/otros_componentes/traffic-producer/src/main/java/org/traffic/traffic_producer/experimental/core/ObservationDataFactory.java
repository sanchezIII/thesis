package org.traffic.traffic_producer.experimental.core;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
@AllArgsConstructor
public class ObservationDataFactory {
  private Class obsDataClass;
  public ObservationData createObservationData(JsonObject jsonData){
    Class[] typeArgs = {JsonObject.class};
    try{
      Method fromJsonMethod = obsDataClass.getMethod("fromJson", typeArgs);

      return (ObservationData) fromJsonMethod.invoke(jsonData);
    }
    catch(NoSuchMethodException e){
      log.error("La clase no contiene el metodo fromJson. Esto es imposible.");
    } catch (InvocationTargetException e) {
      log.error("Reflect error: InvocationTargetException");
    } catch (IllegalAccessException e) {
      log.error("Reflect error: IllegalAccessException");
    }
    return null;
  }

  public ObservationData createDummyObservationData(){
    try {
      return (ObservationData) obsDataClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException e) {
      log.error("Reflect Error: InstantiationException at createDummyObservationData");
    } catch (IllegalAccessException e) {
      log.error("Reflect Error: IllegalAccessException at createDummyObservationData");
    } catch (InvocationTargetException e) {
      log.error("Reflect Error: InvocationTargetException at createDummyObservationData");
    } catch (NoSuchMethodException e) {
      log.error("Reflect Error: NoSuchMethodException at createDummyObservationData");
    }
    return null;
  }
}
