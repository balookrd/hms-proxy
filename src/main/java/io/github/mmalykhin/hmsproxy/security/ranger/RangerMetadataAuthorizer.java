package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerMetadataAuthorizer implements MetadataAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerMetadataAuthorizer.class);

  private final Map<String, RangerBasePlugin> pluginsByCatalog = new LinkedHashMap<>();

  public RangerMetadataAuthorizer(RangerConfig config, Map<String, CatalogConfig> catalogs) {
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
            pluginsByCatalog.put(catalogName, plugin);
            LOG.info("Initialized RangerBasePlugin for catalog '{}' (serviceName={}, serviceType={}, restUrl={})",
                catalogName, rangerConfig.serviceName(), rangerConfig.serviceType(), rangerConfig.policyRestUrl());
          } catch (Exception e) {
            LOG.error("Failed to initialize RangerBasePlugin for catalog '{}'", catalogName, e);
            throw new RuntimeException("Failed to initialize RangerBasePlugin for catalog '" + catalogName + "': " + e.getMessage(), e);
          }
        }
      }
    }
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

    plugin.init();
    return plugin;
  }

  @Override
  public boolean isDatabaseAllowed(String catalogName, String backendDbName, ImpersonationContext impersonation) {
    if (catalogName == null || backendDbName == null) {
      return true;
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return true;
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      return true;
    }

    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("database", backendDbName);

    RangerAccessRequestImpl request = createAccessRequest(resource, impersonation, "select");
    RangerAccessResult result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    if (result != null && result.getIsAllowed()) {
      return true;
    }

    // Try fallback access types "read" and "use"
    request = createAccessRequest(resource, impersonation, "read");
    result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    if (result != null && result.getIsAllowed()) {
      return true;
    }

    request = createAccessRequest(resource, impersonation, "use");
    result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    return result != null && result.getIsAllowed();
  }

  @Override
  public List<String> filterDatabases(String catalogName, List<String> backendDbNames, ImpersonationContext impersonation) {
    if (backendDbNames == null || backendDbNames.isEmpty()) {
      return List.of();
    }
    RangerBasePlugin plugin = catalogName == null ? null : pluginsByCatalog.get(catalogName);
    if (plugin == null || impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      return new ArrayList<>(backendDbNames);
    }
    List<String> visible = new ArrayList<>(backendDbNames.size());
    for (String dbName : backendDbNames) {
      if (isDatabaseAllowed(catalogName, dbName, impersonation)) {
        visible.add(dbName);
      }
    }
    return visible;
  }

  @Override
  public boolean isTableAllowed(String catalogName, String backendDbName, String tableName, ImpersonationContext impersonation) {
    if (catalogName == null || backendDbName == null || tableName == null) {
      return true;
    }
    RangerBasePlugin plugin = pluginsByCatalog.get(catalogName);
    if (plugin == null) {
      return true;
    }
    if (impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      return true;
    }

    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("database", backendDbName);
    resource.setValue("table", tableName);

    RangerAccessRequestImpl request = createAccessRequest(resource, impersonation, "select");
    RangerAccessResult result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    if (result != null && result.getIsAllowed()) {
      return true;
    }

    // Try fallback access types "read" and "show"
    request = createAccessRequest(resource, impersonation, "read");
    result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    if (result != null && result.getIsAllowed()) {
      return true;
    }

    request = createAccessRequest(resource, impersonation, "show");
    result = plugin.isAccessAllowed(request, RangerAuditSuppressor.INSTANCE);
    return result != null && result.getIsAllowed();
  }

  @Override
  public List<String> filterTables(String catalogName, String backendDbName, List<String> tableNames, ImpersonationContext impersonation) {
    if (tableNames == null || tableNames.isEmpty()) {
      return List.of();
    }
    RangerBasePlugin plugin = catalogName == null ? null : pluginsByCatalog.get(catalogName);
    if (plugin == null || impersonation == null || impersonation.userName() == null || impersonation.userName().isBlank()) {
      return new ArrayList<>(tableNames);
    }
    List<String> visible = new ArrayList<>(tableNames.size());
    for (String tableName : tableNames) {
      if (isTableAllowed(catalogName, backendDbName, tableName, impersonation)) {
        visible.add(tableName);
      }
    }
    return visible;
  }

  @Override
  public void close() {
    for (RangerBasePlugin plugin : pluginsByCatalog.values()) {
      try {
        plugin.cleanup();
      } catch (Exception e) {
        LOG.warn("Error cleaning up RangerBasePlugin for service '{}'", plugin.getServiceName(), e);
      }
    }
    pluginsByCatalog.clear();
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
      request.setUserGroups(new HashSet<>(impersonation.groupNames()));
    }
    request.setAccessType(accessType);
    request.setAccessTime(new Date());
    request.setClientIPAddress(ClientRequestContext.remoteAddress().orElse(null));
    request.setResourceMatchingScope(
        org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
    return request;
  }
}
