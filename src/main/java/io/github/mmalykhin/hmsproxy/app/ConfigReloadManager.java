package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConfigReloadManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigReloadManager.class);

  public record ReloadResult(boolean success, String message) {}

  private final Path configPath;
  private volatile ProxyConfig currentConfig;
  private volatile FileTime lastModifiedTime;
  private final List<Consumer<ProxyConfig>> listeners = new CopyOnWriteArrayList<>();
  private final ScheduledExecutorService scheduler;
  private final ExecutorService reloadExecutor;

  public ConfigReloadManager(Path configPath, ProxyConfig initialConfig) {
    this.configPath = configPath;
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
      ProxyConfig newConfig = ProxyConfigLoader.load(configPath);
      this.currentConfig = newConfig;
      try {
        this.lastModifiedTime = Files.getLastModifiedTime(configPath);
      } catch (IOException e) {
        LOG.warn("Failed to update last modified time for {}: {}", configPath, e.getMessage());
      }

      for (Consumer<ProxyConfig> listener : listeners) {
        try {
          listener.accept(newConfig);
        } catch (Throwable t) {
          LOG.error("Error notifying listener of config reload", t);
        }
      }

      LOG.info("Configuration reloaded successfully from {}", configPath);
      return new ReloadResult(true, "Configuration reloaded successfully");
    } catch (Throwable e) {
      String msg = "Failed to reload configuration: " + e.getMessage();
      LOG.error(msg, e);
      return new ReloadResult(false, msg);
    }
  }

  public void checkFileModified() {
    try {
      if (Files.exists(configPath)) {
        FileTime fileTime = Files.getLastModifiedTime(configPath);
        if (lastModifiedTime != null && fileTime.compareTo(lastModifiedTime) > 0) {
          LOG.info("Detected change in config file {} (modified: {}), initiating reload...",
              configPath, fileTime);
          reload();
        }
      }
    } catch (Throwable t) {
      LOG.warn("Failed to check config file modification time: {}", t.getMessage());
    }
  }

  public void triggerReloadAsync() {
    reloadExecutor.submit(this::reload);
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
      Class<?> signalClass = Class.forName("sun.misc.Signal");
      Class<?> signalHandlerClass = Class.forName("sun.misc.SignalHandler");
      Object signal = signalClass.getConstructor(String.class).newInstance("HUP");

      Object handler = Proxy.newProxyInstance(
          signalHandlerClass.getClassLoader(),
          new Class<?>[] {signalHandlerClass},
          new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
              if ("handle".equals(method.getName())) {
                LOG.info("Received SIGHUP signal, triggering configuration reload");
                triggerReloadAsync();
              }
              return null;
            }
          });

      Method handleMethod = signalClass.getMethod("handle", signalClass, signalHandlerClass);
      handleMethod.invoke(null, signal, handler);
      LOG.info("SIGHUP signal handler successfully registered for configuration reload");
    } catch (Throwable t) {
      LOG.debug("SIGHUP signal handler not registered: {}", t.getMessage());
    }
  }

  @Override
  public void close() {
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
