package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.io.File;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerMetadataAuthorizer implements MetadataAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerMetadataAuthorizer.class);
  private static final int MAX_CME_RETRIES = 2;

  private final Map<String, RangerBasePlugin> pluginsByCatalog;
  private final io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics;
  private volatile boolean closed;

  public RangerMetadataAuthorizer(RangerConfig config, Map<String, CatalogConfig> catalogs) {
    this(config, catalogs, null);
  }

  public RangerMetadataAuthorizer(
      RangerConfig config,
      Map<String, CatalogConfig> catalogs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this.metrics = metrics;
    Map<String, RangerBasePlugin> initialized = new LinkedHashMap<>();
    if (catalogs != null) {
      for (Map.Entry<String, CatalogConfig> entry : catalogs.entrySet()) {
        String catalogName = entry.getKey();
        CatalogConfig catConfig = entry.getValue();
        CatalogRangerConfig rangerConfig = catConfig.ranger() != null && catConfig.ranger().enabled()
            ? catConfig.ranger()
            : config.forCatalog(catalogName);

        if (rangerConfig.enabled()) {
          try {
            RangerBasePlugin plugin = createPlugin(catalogName, rangerConfig);
            initialized.put(catalogName, plugin);
            if (metrics != null) {
              metrics.setRangerPluginInfo(
                  catalogName,
                  plugin.getServiceName(),
                  plugin.getServiceType(),
                  plugin.getAppId());
            }
            LOG.info("Initialized RangerBasePlugin for catalog '{}' (serviceName={}, serviceType={}, restUrl={})",
                catalogName, rangerConfig.serviceName(), rangerConfig.serviceType(), rangerConfig.policyRestUrl());
          } catch (Exception e) {
            LOG.error("Failed to initialize RangerBasePlugin for catalog '{}'", catalogName, e);
            throw new RuntimeException("Failed to initialize RangerBasePlugin for catalog '" + catalogName + "': " + e.getMessage(), e);
          }
        }
      }
    }
    this.pluginsByCatalog = Map.copyOf(initialized);
  }

  protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
    String serviceName = config.serviceName() != null && !config.serviceName().isBlank()
        ? config.serviceName()
        : catalogName;
    RangerBasePlugin plugin = new RangerBasePlugin(config.serviceType(), serviceName, config.appId());
    RangerPluginConfig pluginConfig = plugin.getConfig();

    String restUrl = config.policyRestUrl() != null && !config.policyRestUrl().isBlank()
        ? config.policyRestUrl()
        : "http://localhost:6080";
    pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.rest.url", restUrl);
    if (config.policyCacheDir() != null && !config.policyCacheDir().isBlank()) {
      pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.cache.dir", config.policyCacheDir());
    }
    pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.pollIntervalMs",
        String.valueOf(config.policyPollIntervalMs()));
    pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.rest.client.connection.timeoutMs",
        String.valueOf(config.connectionTimeoutMs()));
    pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.rest.client.read.timeoutMs",
        String.valueOf(config.readTimeoutMs()));
    pluginConfig.set("ranger.plugin." + config.serviceType() + ".service.name", serviceName);

    if (config.sslTruststoreFile() != null && !config.sslTruststoreFile().isBlank()) {
      pluginConfig.set("ranger.plugin." + config.serviceType() + ".ssl.truststore.file", config.sslTruststoreFile());
    }
    if (config.sslTruststorePassword() != null && !config.sslTruststorePassword().isBlank()) {
      pluginConfig.set("ranger.plugin." + config.serviceType() + ".ssl.truststore.password", config.sslTruststorePassword());
    }
    if (config.configDir() != null && !config.configDir().isBlank()) {
      File dir = new File(config.configDir());
      if (dir.isDirectory()) {
        File secFile = new File(dir, "ranger-" + config.serviceType() + "-security.xml");
        if (secFile.exists()) {
          pluginConfig.addResource(new org.apache.hadoop.fs.Path(secFile.getAbsolutePath()));
        }
        File auditFile = new File(dir, "ranger-" + config.serviceType() + "-audit.xml");
        if (auditFile.exists()) {
          pluginConfig.addResource(new org.apache.hadoop.fs.Path(auditFile.getAbsolutePath()));
        }
        File sslFile = new File(dir, "ranger-policymgr-ssl.xml");
        if (sslFile.exists()) {
          pluginConfig.addResource(new org.apache.hadoop.fs.Path(sslFile.getAbsolutePath()));
        }
      }
    }

    if (!config.rolesEnabled()) {
      pluginConfig.set("ranger.plugin." + config.serviceType() + ".policy.source.impl",
          NoOpRolesRangerAdminClient.class.getName());
    }

    plugin.init();
    return plugin;
  }

  @Override
  public boolean isDatabaseAllowed(String catalogName, String backendDbName, ImpersonationContext impersonation) {
    if (closed || catalogName == null) {
      return false;
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return true;
    }
    if (backendDbName == null || backendDbName.isBlank()) {
      return false;
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      LOG.warn("Denying Ranger database check for catalog '{}' db='{}' because impersonation context is missing",
          catalogName, backendDbName);
      return false;
    }

    long startNanos = System.nanoTime();
    String accessTypeUsed = "select";
    boolean allowed = false;

    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("database", backendDbName);

    RangerAccessRequestImpl request = createAccessRequest(resource, impersonation, "select");
    RangerAccessResult result = evaluateAccessWithRetry(plugin, request);
    if (result != null && result.getIsAllowed()) {
      allowed = true;
    } else {
      // Try fallback access types "read" and "use"
      request = createAccessRequest(resource, impersonation, "read");
      result = evaluateAccessWithRetry(plugin, request);
      if (result != null && result.getIsAllowed()) {
        accessTypeUsed = "read";
        allowed = true;
      } else {
        request = createAccessRequest(resource, impersonation, "use");
        result = evaluateAccessWithRetry(plugin, request);
        if (result != null && result.getIsAllowed()) {
          accessTypeUsed = "use";
          allowed = true;
        }
      }
    }

    if (metrics != null) {
      double durationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
      metrics.recordRangerEvaluation(
          catalogName,
          "database",
          accessTypeUsed,
          allowed ? "allowed" : "denied",
          durationSeconds);
    }
    return allowed;
  }

  @Override
  public List<String> filterDatabases(String catalogName, List<String> backendDbNames, ImpersonationContext impersonation) {
    if (backendDbNames == null || backendDbNames.isEmpty()) {
      return List.of();
    }
    if (closed || catalogName == null) {
      return List.of();
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return new ArrayList<>(backendDbNames);
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      LOG.warn("Filtering out all {} database(s) for catalog '{}' because impersonation context is missing",
          backendDbNames.size(), catalogName);
      if (metrics != null) {
        metrics.recordRangerFilteredObjects(catalogName, "database", backendDbNames.size());
      }
      return List.of();
    }
    List<String> visible = new ArrayList<>(backendDbNames.size());
    int filtered = 0;
    for (String dbName : backendDbNames) {
      if (isDatabaseAllowed(catalogName, dbName, impersonation)) {
        visible.add(dbName);
      } else {
        filtered++;
      }
    }
    if (metrics != null && filtered > 0) {
      metrics.recordRangerFilteredObjects(catalogName, "database", filtered);
    }
    return visible;
  }

  @Override
  public boolean isTableAllowed(String catalogName, String backendDbName, String tableName, ImpersonationContext impersonation) {
    if (closed || catalogName == null) {
      return false;
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return true;
    }
    if (backendDbName == null || backendDbName.isBlank() || tableName == null || tableName.isBlank()) {
      return false;
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      LOG.warn("Denying Ranger table check for catalog '{}' db='{}' table='{}' because impersonation context is missing",
          catalogName, backendDbName, tableName);
      return false;
    }

    long startNanos = System.nanoTime();
    String accessTypeUsed = "select";
    boolean allowed = false;

    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("database", backendDbName);
    resource.setValue("table", tableName);

    RangerAccessRequestImpl request = createAccessRequest(resource, impersonation, "select");
    RangerAccessResult result = evaluateAccessWithRetry(plugin, request);
    if (result != null && result.getIsAllowed()) {
      allowed = true;
    } else {
      // Try fallback access types "read" and "show"
      request = createAccessRequest(resource, impersonation, "read");
      result = evaluateAccessWithRetry(plugin, request);
      if (result != null && result.getIsAllowed()) {
        accessTypeUsed = "read";
        allowed = true;
      } else {
        request = createAccessRequest(resource, impersonation, "show");
        result = evaluateAccessWithRetry(plugin, request);
        if (result != null && result.getIsAllowed()) {
          accessTypeUsed = "show";
          allowed = true;
        }
      }
    }

    if (metrics != null) {
      double durationSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
      metrics.recordRangerEvaluation(
          catalogName,
          "table",
          accessTypeUsed,
          allowed ? "allowed" : "denied",
          durationSeconds);
    }
    return allowed;
  }

  @Override
  public List<String> filterTables(String catalogName, String backendDbName, List<String> tableNames, ImpersonationContext impersonation) {
    if (tableNames == null || tableNames.isEmpty()) {
      return List.of();
    }
    if (closed || catalogName == null) {
      return List.of();
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return new ArrayList<>(tableNames);
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      LOG.warn("Filtering out all {} table(s) for catalog '{}' db='{}' because impersonation context is missing",
          tableNames.size(), catalogName, backendDbName);
      if (metrics != null) {
        metrics.recordRangerFilteredObjects(catalogName, "table", tableNames.size());
      }
      return List.of();
    }
    List<String> visible = new ArrayList<>(tableNames.size());
    int filtered = 0;
    for (String tableName : tableNames) {
      if (isTableAllowed(catalogName, backendDbName, tableName, impersonation)) {
        visible.add(tableName);
      } else {
        filtered++;
      }
    }
    if (metrics != null && filtered > 0) {
      metrics.recordRangerFilteredObjects(catalogName, "table", filtered);
    }
    return visible;
  }

  protected RangerAccessResult evaluateAccess(RangerBasePlugin plugin, RangerAccessRequest request) {
    return plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
  }

  private RangerAccessResult evaluateAccessWithRetry(RangerBasePlugin plugin, RangerAccessRequest request) {
    for (int attempt = 0; attempt <= MAX_CME_RETRIES; attempt++) {
      try {
        return evaluateAccess(plugin, request);
      } catch (ConcurrentModificationException e) {
        if (attempt < MAX_CME_RETRIES) {
          LOG.warn("ConcurrentModificationException in Ranger SDK during access check for user '{}', retrying ({}/{})",
              request.getUser(), attempt + 1, MAX_CME_RETRIES, e);
          Thread.yield();
        } else {
          LOG.error("Persistent ConcurrentModificationException in Ranger SDK during access check for user '{}' "
                  + "after {} retries; denying access (fail-closed)",
              request.getUser(), MAX_CME_RETRIES, e);
          return createFallbackDeniedResult(request);
        }
      } catch (Error error) {
        throw error;
      } catch (Throwable t) {
        LOG.error("Unexpected error in Ranger SDK during access check for user '{}'; denying access (fail-closed)",
            request.getUser(), t);
        return createFallbackDeniedResult(request);
      }
    }
    return createFallbackDeniedResult(request);
  }

  private static RangerAccessResult createFallbackDeniedResult(RangerAccessRequest request) {
    RangerAccessResult result = new RangerAccessResult(0, "hms-proxy", null, request);
    result.setIsAllowed(false);
    return result;
  }

  @Override
  public void close() {
    closed = true;
    for (RangerBasePlugin plugin : pluginsByCatalog.values()) {
      try {
        plugin.cleanup();
      } catch (Exception e) {
        LOG.warn("Error cleaning up RangerBasePlugin for service '{}'", plugin.getServiceName(), e);
      }
    }
  }

  private static RangerAccessRequestImpl createAccessRequest(
      RangerAccessResourceImpl resource,
      ImpersonationContext impersonation,
      String accessType
  ) {
    RangerAccessRequestImpl request = new RangerAccessRequestImpl();
    request.setResource(resource);
    request.setUser(impersonation.userName());
    if (impersonation.groupNames() != null && !impersonation.groupNames().isEmpty()) {
      Set<String> userGroups = new LinkedHashSet<>();
      for (String g : impersonation.groupNames()) {
        if (g != null && !g.isBlank()) {
          userGroups.add(g);
          userGroups.add(g.toLowerCase(Locale.ROOT));
          if (g.regionMatches(true, 0, "cn=", 0, 3)) {
            int comma = g.indexOf(',');
            String cn = comma > 3 ? g.substring(3, comma) : g.substring(3);
            if (!cn.isBlank()) {
              userGroups.add(cn);
              userGroups.add(cn.toLowerCase(Locale.ROOT));
            }
          }
        }
      }
      request.setUserGroups(userGroups);
    }
    request.setAccessType(accessType);
    request.setAccessTime(new Date());
    request.setClientIPAddress(ClientRequestContext.remoteAddress().orElse(null));
    request.setResourceMatchingScope(
        org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
    return request;
  }
}
