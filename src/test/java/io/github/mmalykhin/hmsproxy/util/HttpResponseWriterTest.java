package io.github.mmalykhin.hmsproxy.util;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises {@link HttpResponseWriter#sendBody} directly against a recording {@link HttpExchange},
 * asserting the exact {@code (status, contentLength)} pair passed to
 * {@code sendResponseHeaders} and the bytes written to the response body. This is the regression
 * guard for the HEAD "stream closed" bug: {@code ManagementHttpServerTest} can only compare the
 * client-visible HEAD status to the GET status, which stays identical whether or not the HEAD
 * guard exists, so it cannot fail when the guard is removed. This test fails if it is.
 */
public class HttpResponseWriterTest {
  @Test
  public void getSendsContentLengthAndWritesBody() throws IOException {
    RecordingHttpExchange exchange = new RecordingHttpExchange("GET");
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

    HttpResponseWriter.sendBody(exchange, 200, bytes);

    Assert.assertEquals(200, exchange.sentStatus);
    Assert.assertEquals(bytes.length, exchange.sentContentLength);
    Assert.assertArrayEquals(bytes, exchange.responseBody.toByteArray());
    Assert.assertTrue("response body must be closed", exchange.responseBodyClosed);
  }

  @Test
  public void headSendsNoContentLengthAndWritesNoBody() throws IOException {
    RecordingHttpExchange exchange = new RecordingHttpExchange("HEAD");
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

    HttpResponseWriter.sendBody(exchange, 200, bytes);

    Assert.assertEquals(200, exchange.sentStatus);
    Assert.assertEquals(-1, exchange.sentContentLength);
    Assert.assertEquals(0, exchange.responseBody.toByteArray().length);
    Assert.assertTrue("response body must be closed", exchange.responseBodyClosed);
  }

  @Test
  public void headIsCaseInsensitive() throws IOException {
    RecordingHttpExchange exchange = new RecordingHttpExchange("head");
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

    HttpResponseWriter.sendBody(exchange, 204, bytes);

    Assert.assertEquals(204, exchange.sentStatus);
    Assert.assertEquals(-1, exchange.sentContentLength);
    Assert.assertEquals(0, exchange.responseBody.toByteArray().length);
  }

  /**
   * Minimal recording fake: captures the arguments to {@code sendResponseHeaders} and every byte
   * written to the response body. Every other method is unused by {@link HttpResponseWriter} and
   * throws if called, so a regression that starts touching them is caught immediately.
   */
  private static final class RecordingHttpExchange extends HttpExchange {
    private final String requestMethod;
    private final ByteArrayOutputStream responseBody = new NonClosingByteArrayOutputStream();
    private boolean responseBodyClosed;
    private int sentStatus = -1;
    private long sentContentLength = Long.MIN_VALUE;

    private RecordingHttpExchange(String requestMethod) {
      this.requestMethod = requestMethod;
    }

    @Override
    public String getRequestMethod() {
      return requestMethod;
    }

    @Override
    public void sendResponseHeaders(int status, long contentLength) {
      this.sentStatus = status;
      this.sentContentLength = contentLength;
    }

    @Override
    public OutputStream getResponseBody() {
      return responseBody;
    }

    @Override
    public Headers getRequestHeaders() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Headers getResponseHeaders() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.net.URI getRequestURI() {
      throw new UnsupportedOperationException();
    }

    @Override
    public HttpContext getHttpContext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getRequestBody() {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getResponseCode() {
      return sentStatus;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getProtocol() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setStreams(InputStream i, OutputStream o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public HttpPrincipal getPrincipal() {
      throw new UnsupportedOperationException();
    }

    /**
     * {@link HttpResponseWriter#sendBody} closes the response body in a try-with-resources; a
     * plain {@link ByteArrayOutputStream#close()} is a no-op, so this override records that the
     * close actually happened without losing the buffered bytes.
     */
    private final class NonClosingByteArrayOutputStream extends ByteArrayOutputStream {
      @Override
      public void close() {
        responseBodyClosed = true;
      }
    }
  }
}
