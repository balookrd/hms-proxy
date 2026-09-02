package io.github.mmalykhin.hmsproxy.security.ranger;

import java.util.Collection;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;

public final class RangerAuditSuppressor implements RangerAccessResultProcessor {
  public static final RangerAuditSuppressor INSTANCE = new RangerAuditSuppressor();

  private RangerAuditSuppressor() {
  }

  @Override
  public void processResult(RangerAccessResult result) {
    // No-op: suppress audit logging during metadata listings
  }

  @Override
  public void processResults(Collection<RangerAccessResult> results) {
    // No-op
  }
}
