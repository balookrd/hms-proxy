package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfileResolver;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;

public final class FrontendProcessorFactory {
  private FrontendProcessorFactory() {
  }

  public static TProcessor create(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    MetastoreRuntimeProfile runtimeProfile =
        MetastoreRuntimeProfileResolver.forFrontendProfile(config.compatibility().frontendProfile());
    return switch (runtimeProfile) {
      case APACHE_3_1_3 -> ApacheFrontendBridge.createProcessor(config, apacheHandler);
      case HORTONWORKS_3_1_0_3_1_0_78, HORTONWORKS_3_1_0_3_1_5_6150_1 ->
          HortonworksFrontendBridge.createProcessor(config, apacheHandler);
    };
  }
}
