package io.github.mmalykhin.hmsproxy.security.ranger;

import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.util.RangerRoles;

/**
 * Ranger Admin REST client implementation that suppresses periodic roles polling.
 *
 * <p>When Ranger roles are disabled or unsupported by the target Ranger Admin deployment,
 * this client returns {@code null} from {@link #getRolesIfUpdated(long, long)} without
 * issuing HTTP GET requests to the {@code /service/roles/download/{serviceName}} endpoint,
 * preventing noisy 404 HTTP errors in the logs.
 */
public class NoOpRolesRangerAdminClient extends RangerAdminRESTClient {

  @Override
  public RangerRoles getRolesIfUpdated(long lastKnownRoleVersion, long lastActivationTimeInMillis) {
    // Roles polling is disabled; return null to signal no role updates without HTTP calls
    return null;
  }
}
