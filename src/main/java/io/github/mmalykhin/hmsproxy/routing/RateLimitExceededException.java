package io.github.mmalykhin.hmsproxy.routing;

import org.apache.hadoop.hive.metastore.api.MetaException;

final class RateLimitExceededException extends MetaException {
  RateLimitExceededException(String message) {
    super(message);
  }
}
