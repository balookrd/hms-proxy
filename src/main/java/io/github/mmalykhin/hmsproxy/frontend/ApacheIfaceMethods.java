package io.github.mmalykhin.hmsproxy.frontend;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/**
 * Name/arity index over the Apache metastore interface, used by the front-door bridges to map an
 * incoming frontend RPC onto its Apache counterpart. Built once instead of cloning the
 * {@code getMethods()} array on every bridged call.
 */
final class ApacheIfaceMethods {
  private static final Map<String, Method> INDEX = buildIndex();

  private ApacheIfaceMethods() {}

  static Method find(String methodName, int argumentCount) {
    return INDEX.get(key(methodName, argumentCount));
  }

  private static Map<String, Method> buildIndex() {
    Map<String, Method> index = new HashMap<>();
    for (Method candidate : ThriftHiveMetastore.Iface.class.getMethods()) {
      index.putIfAbsent(key(candidate.getName(), candidate.getParameterCount()), candidate);
    }
    return Map.copyOf(index);
  }

  private static String key(String methodName, int argumentCount) {
    return methodName + '/' + argumentCount;
  }
}
