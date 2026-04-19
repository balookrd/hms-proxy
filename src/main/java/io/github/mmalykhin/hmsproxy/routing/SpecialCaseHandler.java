package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;

@FunctionalInterface
interface SpecialCaseHandler {
  Object handle(Method method, Object[] args) throws Throwable;
}
