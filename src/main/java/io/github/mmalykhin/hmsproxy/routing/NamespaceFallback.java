package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;

/**
 * Escape hatch used by special-case handlers that want to delegate to the generic
 * namespace-routing machinery in {@link RoutingHandler} without depending on it directly.
 */
interface NamespaceFallback {
  Object invokeGlobal(Method method, Object[] args) throws Throwable;

  Object routeByNamespaceOrFail(Method method, Object[] args) throws Throwable;
}
