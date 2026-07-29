package io.github.mmalykhin.hmsproxy.restcatalog;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.iceberg.rest.RESTSerializers;

/**
 * Mirror of Iceberg's package-private RESTObjectMapper. We construct an equivalent
 * Jackson ObjectMapper here so the handler does not need to live in
 * org.apache.iceberg.rest. registerAll() is the only public touchpoint we rely on.
 */
final class IcebergRestMapper {
  private static final ObjectMapper MAPPER;

  static {
    ObjectMapper m = new ObjectMapper();
    m.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    m.setPropertyNamingStrategy(new PropertyNamingStrategy.KebabCaseStrategy());
    RESTSerializers.registerAll(m);
    MAPPER = m;
  }

  private IcebergRestMapper() {
  }

  static ObjectMapper mapper() {
    return MAPPER;
  }
}
