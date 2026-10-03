package io.github.mmalykhin.hmsproxy.security.groups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * An individual user group cache record stored in memory and persisted to disk.
 */
public record CachedUserGroups(
    @JsonProperty("groups") List<String> groups,
    @JsonProperty("updatedAtEpochMs") long updatedAtEpochMs
) {
  @JsonCreator
  public CachedUserGroups(
      @JsonProperty("groups") List<String> groups,
      @JsonProperty("updatedAtEpochMs") long updatedAtEpochMs
  ) {
    this.groups = groups == null ? List.of() : List.copyOf(groups);
    this.updatedAtEpochMs = updatedAtEpochMs;
  }
}
