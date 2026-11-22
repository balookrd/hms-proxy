package io.github.mmalykhin.hmsproxy.backend;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Caches reflective metastore method lookups per interface class. {@code Class.getMethod} scans the
 * ~200 method thrift interface and returns a defensive copy on every call, which is per-RPC cost on
 * the invocation hot path. The cache is keyed by the interface class so it survives session churn
 * and stays scoped to the classloader that defined the interface.
 */
final class ThriftMethodCache {
  private static final ClassValue<ConcurrentMap<MethodKey, Method>> CACHE =
      new ClassValue<>() {
        @Override
        protected ConcurrentMap<MethodKey, Method> computeValue(Class<?> type) {
          return new ConcurrentHashMap<>();
        }
      };

  private static final String[] EMPTY_TYPE_NAMES = new String[0];

  private ThriftMethodCache() {}

  static Method lookup(
      Class<?> ifaceClass,
      String methodName,
      Class<?>[] parameterTypes,
      MethodResolver resolver
  ) throws NoSuchMethodException {
    ConcurrentMap<MethodKey, Method> cache = CACHE.get(ifaceClass);
    MethodKey key = new MethodKey(methodName, toTypeNames(parameterTypes));
    Method cached = cache.get(key);
    if (cached != null) {
      return cached;
    }
    Method resolved = resolver.resolve();
    cache.putIfAbsent(key, resolved);
    return resolved;
  }

  private static String[] toTypeNames(Class<?>[] parameterTypes) {
    if (parameterTypes == null || parameterTypes.length == 0) {
      return EMPTY_TYPE_NAMES;
    }
    String[] names = new String[parameterTypes.length];
    for (int i = 0; i < parameterTypes.length; i++) {
      names[i] = parameterTypes[i].getName();
    }
    return names;
  }

  @FunctionalInterface
  interface MethodResolver {
    Method resolve() throws NoSuchMethodException;
  }

  private record MethodKey(String name, String[] parameterTypeNames) {
    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof MethodKey that)) {
        return false;
      }
      return name.equals(that.name) && Arrays.equals(parameterTypeNames, that.parameterTypeNames);
    }

    @Override
    public int hashCode() {
      return 31 * name.hashCode() + Arrays.hashCode(parameterTypeNames);
    }
  }
}
