package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Root container structure for disk-serialized group cache data.
 */
public record DiskCachePayload(
    @JsonProperty("version") int version,
    @JsonProperty("savedAtEpochMs") long savedAtEpochMs,
    @JsonProperty("entries") Map<String, CachedUserGroups> entries
) {
  public static final int CURRENT_VERSION = 1;

  @JsonCreator
  public DiskCachePayload(
      @JsonProperty("version") int version,
      @JsonProperty("savedAtEpochMs") long savedAtEpochMs,
      @JsonProperty("entries") Map<String, CachedUserGroups> entries
  ) {
    this.version = version;
    this.savedAtEpochMs = savedAtEpochMs;
    this.entries = entries == null ? Map.of() : Map.copyOf(entries);
  }
}
