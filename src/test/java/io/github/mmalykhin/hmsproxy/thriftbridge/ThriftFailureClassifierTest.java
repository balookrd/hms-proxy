package io.github.mmalykhin.hmsproxy.thriftbridge;

import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class ThriftFailureClassifierTest {
  @Test
  public void onlyUnknownMethodTypeMeansTheBackendHasNoSuchMethod() {
    Assert.assertTrue(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "get_table_req")));
    Assert.assertTrue(ThriftFailureClassifier.isUnsupportedMethod(new NoSuchMethodException("get_table_req")));
    Assert.assertTrue(ThriftFailureClassifier.isUnsupportedMethod(new NoSuchMethodError("get_table_req")));

    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.MISSING_RESULT, "no result")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE, "bad message")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.WRONG_METHOD_NAME, "other method")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.INVALID_PROTOCOL, "bad protocol")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(
        new TApplicationException(TApplicationException.UNSUPPORTED_CLIENT_TYPE, "bad client")));
    // The no-type constructor defaults to UNKNOWN, which says nothing about the method.
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(new TApplicationException("unsupported")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(new TTransportException("closed")));
    Assert.assertFalse(ThriftFailureClassifier.isUnsupportedMethod(new MetaException("boom")));
  }

  @Test
  public void transportFailuresStayASeparateCategory() {
    Assert.assertTrue(ThriftFailureClassifier.isTransportFailure(new TTransportException("closed")));
    Assert.assertFalse(ThriftFailureClassifier.isTransportFailure(
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "get_table_req")));
    Assert.assertFalse(ThriftFailureClassifier.isTransportFailure(
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")));
    Assert.assertFalse(ThriftFailureClassifier.isTransportFailure(new MetaException("boom")));
  }

  @Test
  public void onlyStreamDesyncTypesPoisonTheConnection() {
    Assert.assertTrue(ThriftFailureClassifier.isProtocolDesync(
        new TApplicationException(TApplicationException.WRONG_METHOD_NAME, "other method")));
    Assert.assertTrue(ThriftFailureClassifier.isProtocolDesync(
        new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, "seq")));
    Assert.assertTrue(ThriftFailureClassifier.isProtocolDesync(
        new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE, "bad message")));

    Assert.assertFalse(ThriftFailureClassifier.isProtocolDesync(
        new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")));
    Assert.assertFalse(ThriftFailureClassifier.isProtocolDesync(
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "get_table_req")));
    Assert.assertFalse(ThriftFailureClassifier.isProtocolDesync(new TTransportException("closed")));
  }

  @Test
  public void reflectiveWrappersAreUnwrapped() {
    Assert.assertTrue(ThriftFailureClassifier.isUnsupportedMethod(new InvocationTargetException(
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "get_table_req"))));
    Assert.assertTrue(ThriftFailureClassifier.isTransportFailure(
        new InvocationTargetException(new TTransportException("closed"))));
    // A MetaException that merely carries a transport cause is still a backend-reported failure.
    MetaException wrapped = new MetaException("open failed");
    wrapped.initCause(new TTransportException("closed"));
    Assert.assertFalse(ThriftFailureClassifier.isTransportFailure(wrapped));
  }
}
