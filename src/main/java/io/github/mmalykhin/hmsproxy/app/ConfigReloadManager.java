package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ConfigReloadScope;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConfigReloadManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigReloadManager.class);

  public record ReloadResult(boolean success, String message) {}

  private final Path configPath;
  // The settings a reload cannot apply keep running with these values until a restart.
  private final ProxyConfig startupConfig;
  private volatile ProxyConfig currentConfig;
  private volatile FileTime lastModifiedTime;
  private final List<Consumer<ProxyConfig>> listeners = new CopyOnWriteArrayList<>();
  private final ScheduledExecutorService scheduler;
  private final ExecutorService reloadExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Class<?> signalClass;
  private Class<?> signalHandlerClass;
  private Object sighupSignal;
  private Object registeredSighupHandler;
  private Object previousSighupHandler;

  public ConfigReloadManager(Path configPath, ProxyConfig initialConfig) {
    this.configPath = configPath;
    this.startupConfig = initialConfig;
    this.currentConfig = initialConfig;
    try {
      if (Files.exists(configPath)) {
        this.lastModifiedTime = Files.getLastModifiedTime(configPath);
      }
    } catch (IOException e) {
      LOG.warn("Could not read initial modification time for {}: {}", configPath, e.getMessage());
    }

    this.reloadExecutor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(runnable, "hms-proxy-config-reload-worker");
      thread.setDaemon(true);
      return thread;
    });

    this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "hms-proxy-config-reloader");
      thread.setDaemon(true);
      return thread;
    });

    registerSignalHandler();

    long pollInterval = initialConfig != null ? initialConfig.reloadPollIntervalSeconds() : 5L;
    if (pollInterval > 0) {
      this.scheduler.scheduleWithFixedDelay(
          this::checkFileModified,
          pollInterval,
          pollInterval,
          TimeUnit.SECONDS);
      LOG.info("Config reload file poller started with interval {}s for {}", pollInterval, configPath);
    } else {
      LOG.info("Config reload file poller disabled (interval <= 0) for {}", configPath);
    }
  }

  public void addListener(Consumer<ProxyConfig> listener) {
    listeners.add(listener);
  }

  public synchronized ReloadResult reload() {
    LOG.info("Reloading configuration from {}...", configPath);
    try {
      if (!Files.exists(configPath)) {
        String msg = "Config file does not exist: " + configPath;
        LOG.error(msg);
        return new ReloadResult(false, msg);
      }
      ProxyConfig previousConfig = this.currentConfig;
      ProxyConfig newConfig = ProxyConfigLoader.load(configPath);
      rememberModificationTime();

      List<Consumer<ProxyConfig>> appliedListeners = new ArrayList<>(listeners.size());
      try {
        for (Consumer<ProxyConfig> listener : listeners) {
          listener.accept(newConfig);
          appliedListeners.add(listener);
        }
      } catch (Throwable t) {
        LOG.error("Error notifying listener of config reload; rolling back {} listener(s)",
            appliedListeners.size(), t);
        if (previousConfig != null) {
          for (int i = appliedListeners.size() - 1; i >= 0; i--) {
            try {
              appliedListeners.get(i).accept(previousConfig);
            } catch (Throwable rollbackError) {
              t.addSuppressed(rollbackError);
              LOG.error("Failed to roll back config reload listener", rollbackError);
            }
          }
        }
        this.currentConfig = previousConfig;
        return new ReloadResult(false, "Failed to apply reloaded configuration: " + t.getMessage());
      }

      this.currentConfig = newConfig;

      List<String> notApplied = startupConfig == null
          ? List.of()
          : ConfigReloadScope.changesRequiringRestart(startupConfig, newConfig);
      if (notApplied.isEmpty()) {
        LOG.info("Configuration reloaded successfully from {}", configPath);
        return new ReloadResult(true, "Configuration reloaded successfully");
      }
      String msg = "Configuration reloaded from " + configPath + ", but these settings differ from the running"
          + " ones and take effect only after a restart: " + String.join(", ", notApplied);
      LOG.warn(msg);
      return new ReloadResult(true, msg);
    } catch (Throwable e) {
      // Remember the broken file's mtime: the poller only retries once the file changes again
      // instead of re-parsing and logging the same error on every interval.
      rememberModificationTime();
      String msg = "Failed to reload configuration: " + e.getMessage();
      LOG.error(msg, e);
      return new ReloadResult(false, msg);
    }
  }

  private void rememberModificationTime() {
    try {
      this.lastModifiedTime = Files.getLastModifiedTime(configPath);
    } catch (IOException e) {
      LOG.warn("Failed to update last modified time for {}: {}", configPath, e.getMessage());
    }
  }

  public void checkFileModified() {
    try {
      if (Files.exists(configPath)) {
        FileTime fileTime = Files.getLastModifiedTime(configPath);
        FileTime previousTime = this.lastModifiedTime;
        if (previousTime == null) {
          LOG.info("Config file {} appeared (modified: {}), initiating reload...", configPath, fileTime);
          reload();
        } else if (!fileTime.equals(previousTime)) {
          LOG.info("Detected change in config file {} (modified: {} -> {}), initiating reload...",
              configPath, previousTime, fileTime);
          reload();
        }
      }
    } catch (Throwable t) {
      LOG.warn("Failed to check config file modification time: {}", t.getMessage());
    }
  }

  public void triggerReloadAsync() {
    if (!closed.get()) {
      reloadExecutor.submit(this::reload);
    }
  }

  public ProxyConfig currentConfig() {
    return currentConfig;
  }

  public Path configPath() {
    return configPath;
  }

  public FileTime lastModifiedTime() {
    return lastModifiedTime;
  }

  private void registerSignalHandler() {
    try {
      Class<?> sigClass = Class.forName("sun.misc.Signal");
      Class<?> handlerClass = Class.forName("sun.misc.SignalHandler");
      Object signal = sigClass.getConstructor(String.class).newInstance("HUP");

      Object handler = Proxy.newProxyInstance(
          handlerClass.getClassLoader(),
          new Class<?>[] {handlerClass},
          new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
              if ("handle".equals(method.getName()) && !closed.get()) {
                LOG.info("Received SIGHUP signal, triggering configuration reload");
                triggerReloadAsync();
              }
              return null;
            }
          });

      Method handleMethod = sigClass.getMethod("handle", sigClass, handlerClass);
      Object previous = handleMethod.invoke(null, signal, handler);
      this.signalClass = sigClass;
      this.signalHandlerClass = handlerClass;
      this.sighupSignal = signal;
      this.registeredSighupHandler = handler;
      this.previousSighupHandler = previous;
      LOG.info("SIGHUP signal handler successfully registered for configuration reload");
    } catch (Throwable t) {
      LOG.debug("SIGHUP signal handler not registered: {}", t.getMessage());
    }
  }

  private void unregisterSignalHandler() {
    if (signalClass == null || signalHandlerClass == null
        || sighupSignal == null || previousSighupHandler == null) {
      return;
    }
    try {
      Method handleMethod = signalClass.getMethod("handle", signalClass, signalHandlerClass);
      Object current = handleMethod.invoke(null, sighupSignal, previousSighupHandler);
      if (current != null && current != registeredSighupHandler) {
        // Another instance installed a newer SIGHUP handler after us — restore it back.
        handleMethod.invoke(null, sighupSignal, current);
      }
    } catch (Throwable t) {
      LOG.debug("Failed to restore previous SIGHUP handler: {}", t.getMessage());
    } finally {
      registeredSighupHandler = null;
      previousSighupHandler = null;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    unregisterSignalHandler();
    scheduler.shutdownNow();
    reloadExecutor.shutdownNow();
    try {
      if (!scheduler.awaitTermination(2L, TimeUnit.SECONDS)) {
        LOG.warn("Config reload scheduler did not terminate within 2s");
      }
      if (!reloadExecutor.awaitTermination(2L, TimeUnit.SECONDS)) {
        LOG.warn("Config reload executor did not terminate within 2s");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
