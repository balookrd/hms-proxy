package io.github.mmalykhin.hmsproxy.observability;

import javax.security.auth.Subject;
import org.junit.Assert;
import org.junit.Test;

public class BackendKerberosLoginTrackerTest {
  @Test
  public void trackerExposesMostRecentBackendLogin() {
    BackendKerberosLoginTracker tracker = new BackendKerberosLoginTracker();
    Subject first = new Subject();
    Subject second = new Subject();

    Assert.assertNull(tracker.lastLogin());
    tracker.record("hive/backend-a@EXAMPLE.COM", first);
    Assert.assertEquals("hive/backend-a@EXAMPLE.COM", tracker.lastLogin().principal());
    Assert.assertSame(first, tracker.lastLogin().subject());

    tracker.record("hive/backend-b@EXAMPLE.COM", second);
    Assert.assertEquals("hive/backend-b@EXAMPLE.COM", tracker.lastLogin().principal());
    Assert.assertSame(second, tracker.lastLogin().subject());
  }

  @Test
  public void loginWithoutSubjectIsStillRecorded() {
    BackendKerberosLoginTracker tracker = new BackendKerberosLoginTracker();

    tracker.record("hive/backend-a@EXAMPLE.COM", null);

    Assert.assertEquals("hive/backend-a@EXAMPLE.COM", tracker.lastLogin().principal());
    Assert.assertNull(tracker.lastLogin().subject());
  }

  @Test
  public void processWideTrackerIsShared() {
    Assert.assertSame(
        BackendKerberosLoginTracker.processWide(),
        BackendKerberosLoginTracker.processWide());
  }
}
