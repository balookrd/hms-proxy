package io.github.mmalykhin.hmsproxy.backend;

import java.util.List;

public record ImpersonationContext(String userName, List<String> groupNames) {
}
