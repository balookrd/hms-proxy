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

  private ThriftMethodCache() {}

  static Method lookup(
      Class<?> ifaceClass,
      String methodName,
      Class<?>[] parameterTypes,
      MethodResolver resolver
  ) throws NoSuchMethodException {
    ConcurrentMap<MethodKey, Method> cache = CACHE.get(ifaceClass);
    Method cached = cache.get(new MethodKey(methodName, parameterTypes));
    if (cached != null) {
      return cached;
    }
    Method resolved = resolver.resolve();
    // Copy the caller-owned parameter array before it becomes part of a cache key.
    cache.putIfAbsent(new MethodKey(methodName, parameterTypes.clone()), resolved);
    return resolved;
  }

  @FunctionalInterface
  interface MethodResolver {
    Method resolve() throws NoSuchMethodException;
  }

  private record MethodKey(String name, Class<?>[] parameterTypes) {
    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof MethodKey that)) {
        return false;
      }
      return name.equals(that.name) && Arrays.equals(parameterTypes, that.parameterTypes);
    }

    @Override
    public int hashCode() {
      return 31 * name.hashCode() + Arrays.hashCode(parameterTypes);
    }
  }
}
