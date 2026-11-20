package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.security.DelegationTokenStore;
import org.junit.Assert;
import org.junit.Test;

public class RoutingMetaStoreProxyErrorHandlingTest {
  @Test
  public void unexpectedRuntimeExceptionIsWrappedInMetaExceptionInsteadOfEscapingToThriftServer() {
    DelegationTokenStore.TokenStoreException rootCause =
        new DelegationTokenStore.TokenStoreException(
            "Error getting children for /hive/hms-proxy-delegation-tokens/METASTORE/tokens",
            new RuntimeException("zk connection loss"));
    InvocationHandler failingChain = (proxy, method, args) -> {
      throw rootCause;
    };
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        new ProxyObservability(RoutingMetaStoreProxyTestSupport.CUSTOM_SEPARATOR_CONFIG),
        null,
        null,
        null,
        failingChain);
    ThriftHiveMetastore.Iface proxy =
        RoutingMetaStoreProxy.newProxy(ThriftHiveMetastore.Iface.class, handler);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> proxy.cancel_delegation_token("token-str"));
    Assert.assertTrue(error.getMessage().contains("cancel_delegation_token"));
    Assert.assertTrue(error.getMessage().contains("Error getting children"));
    Assert.assertSame(rootCause, error.getCause());
  }

  @Test
  public void fatalErrorIsRethrownWithoutWrapping() {
    AssertionError fatal = new AssertionError("fatal invariant violation");
    InvocationHandler failingChain = (proxy, method, args) -> {
      throw fatal;
    };
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        new ProxyObservability(RoutingMetaStoreProxyTestSupport.CUSTOM_SEPARATOR_CONFIG),
        null,
        null,
        null,
        failingChain);
    ThriftHiveMetastore.Iface proxy =
        RoutingMetaStoreProxy.newProxy(ThriftHiveMetastore.Iface.class, handler);

    AssertionError thrown = Assert.assertThrows(
        AssertionError.class,
        () -> proxy.get_all_databases());
    Assert.assertSame(fatal, thrown);
  }
}
