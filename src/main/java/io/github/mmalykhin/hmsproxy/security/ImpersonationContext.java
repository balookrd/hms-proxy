package io.github.mmalykhin.hmsproxy.security;

import java.util.List;

public record ImpersonationContext(String userName, List<String> groupNames) {
}
