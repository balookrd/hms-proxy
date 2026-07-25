package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.InvocationHandler;

final class RoutingPipelineFactory {

  private RoutingPipelineFactory() {}

  record Pipeline(
      SyntheticReadLockManager syntheticReadLockManager,
      BackendRoutingController backendRoutingController,
      RoutingHandler routingHandler,
      InvocationHandler chain
  ) {}

  static Pipeline assemble(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      FrontDoorSecurity frontDoorSecurity,
      ProxyObservability observability,
      ExternalTableDropPurger externalTableDropPurger
  ) {
    CompatibilityLayer compatibilityLayer = new CompatibilityLayer(config, frontDoorSecurity);
    TransactionalTableMutationGuard transactionalTableMutationGuard = new TransactionalTableMutationGuard(config);
    SyntheticReadLockManager syntheticReadLockManager = new SyntheticReadLockManager(config, observability.metrics());
    RequestRateLimiter requestRateLimiter = new RequestRateLimiter(config, observability.metrics());
    BackendRoutingController backendRoutingController = new BackendRoutingController(config, router, observability);
    AdmissionGate admissionGate = new AdmissionGate(backendRoutingController, requestRateLimiter);
    FanoutExecutor fanoutExecutor = new FanoutExecutor(backendRoutingController, router, admissionGate);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        compatibilityLayer, admissionGate, observability, fanoutExecutor);
    long aliveSince = System.currentTimeMillis() / 1000L;
    ImpersonationResolver impersonationResolver = new ImpersonationResolver(config);
    RoutingHandler routingHandler = new RoutingHandler(
        config,
        router,
        federationLayer,
        compatibilityLayer,
        observability,
        dispatcher,
        impersonationResolver,
        externalTableDropPurger);
    CompatibilityHandler compatibilityHandler = new CompatibilityHandler(
        config, compatibilityLayer, router, observability, dispatcher, impersonationResolver, aliveSince,
        routingHandler);
    LockHandler lockHandler = new LockHandler(
        syntheticReadLockManager, admissionGate, router, federationLayer, observability, compatibilityHandler);
    InvocationHandler chain = new RateLimitingHandler(requestRateLimiter, transactionalTableMutationGuard, lockHandler);
    return new Pipeline(syntheticReadLockManager, backendRoutingController, routingHandler, chain);
  }
}
