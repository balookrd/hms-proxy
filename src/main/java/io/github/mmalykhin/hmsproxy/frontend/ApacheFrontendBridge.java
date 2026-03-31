package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;

public final class ApacheFrontendBridge {
  private ApacheFrontendBridge() {
  }

  public static TProcessor createProcessor(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) {
    return new ThriftHiveMetastore.Processor<>(apacheHandler);
  }
}
