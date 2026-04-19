package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

final class GetAllDatabasesHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetAllDatabasesHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : support.invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<String> result = (List<String>) support.dispatcher.invokeDirect(
              backend, method, null, impersonation, requestId, false, false);
          return result;
        })) {
      List<String> backendDatabases = fanoutResult.value();
      for (String database : backendDatabases) {
        if (!support.federationLayer.isDatabaseExposed(fanoutResult.backend().name(), database)) {
          support.recordFilteredObject(method.getName(), fanoutResult.backend().name(), "database");
          continue;
        }
        databases.add(support.federationLayer.externalDatabaseName(fanoutResult.backend().name(), database));
      }
    }
    return databases;
  }
}
