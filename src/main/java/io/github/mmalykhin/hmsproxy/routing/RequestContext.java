package io.github.mmalykhin.hmsproxy.routing;

/**
 * Static holder for per-request ThreadLocal state shared across the routing layer.
 * Set and cleared by {@link RoutingMetaStoreHandler#invoke} for every incoming call.
 */
final class RequestContext {
  static final ThreadLocal<Long> REQUEST_ID = new ThreadLocal<>();
  static final ThreadLocal<RequestObservation> REQUEST_OBSERVATION = new ThreadLocal<>();

  static long currentRequestId() {
    Long id = REQUEST_ID.get();
    return id == null ? -1L : id;
  }

  static RequestObservation currentObservation() {
    RequestObservation obs = REQUEST_OBSERVATION.get();
    return obs == null ? new RequestObservation("unknown") : obs;
  }

  private RequestContext() {
  }
}
