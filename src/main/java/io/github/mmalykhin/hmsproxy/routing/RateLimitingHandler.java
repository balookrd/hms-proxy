package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * First handler in the invocation chain. Enforces request-level rate limits and
 * validates transactional mutation guards before any routing logic runs.
 */
final class RateLimitingHandler implements InvocationHandler {
  private final RequestRateLimiter requestRateLimiter;
  private final TransactionalTableMutationGuard transactionalTableMutationGuard;
  private final InvocationHandler next;

  RateLimitingHandler(
      RequestRateLimiter requestRateLimiter,
      TransactionalTableMutationGuard transactionalTableMutationGuard,
      InvocationHandler next
  ) {
    this.requestRateLimiter = requestRateLimiter;
    this.transactionalTableMutationGuard = transactionalTableMutationGuard;
    this.next = next;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String name = method.getName();
    requestRateLimiter.enforceRequest(name);
    transactionalTableMutationGuard.validate(name, args);
    return next.invoke(proxy, method, args);
  }
}
