package com.griddynamcics.hazelcasttpsreduction;

import com.griddynamcics.hazelcasttpsreduction.verticles.MainVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class Launcher {
	private static final Logger log = LoggerFactory.getLogger(Launcher.class);
	// Environment variables used to configure verticle count and autoscaling behavior.
	private static final String INITIAL_INSTANCES_ENV = "MAIN_VERTICLE_INITIAL_INSTANCES";
	private static final String MAX_INSTANCES_ENV = "MAIN_VERTICLE_MAX_INSTANCES";
	private static final String EVENT_LOOP_THREADS_ENV = "VERTX_EVENT_LOOP_THREADS";
	private static final String AUTOSCALE_ENABLED_ENV = "MAIN_VERTICLE_AUTOSCALE_ENABLED";
	private static final String AUTOSCALE_CHECK_MS_ENV = "MAIN_VERTICLE_AUTOSCALE_CHECK_MS";
	private static final String AUTOSCALE_UP_THRESHOLD_ENV = "MAIN_VERTICLE_AUTOSCALE_UP_THRESHOLD";
	private static final String AUTOSCALE_DOWN_THRESHOLD_ENV = "MAIN_VERTICLE_AUTOSCALE_DOWN_THRESHOLD";
	private static final String TARGET_INFLIGHT_ENV = "MAIN_VERTICLE_TARGET_INFLIGHT_PER_INSTANCE";
	private static final String AUTOSCALE_COOLDOWN_MS_ENV = "MAIN_VERTICLE_AUTOSCALE_COOLDOWN_MS";
	private static final String AUTOSCALE_DOWN_COOLDOWN_MS_ENV = "MAIN_VERTICLE_AUTOSCALE_DOWN_COOLDOWN_MS";
	private static final String AUTOSCALE_LOW_CHECKS_ENV = "MAIN_VERTICLE_AUTOSCALE_LOW_CHECKS";
	private static final int DEFAULT_INITIAL_INSTANCES = 1;
	private static final int DEFAULT_MAX_INSTANCES = Math.max(2, Runtime.getRuntime().availableProcessors());
	private static final int DEFAULT_TARGET_INFLIGHT_PER_INSTANCE = 200;
	private static final long DEFAULT_CHECK_MS = 2000L;
	private static final long DEFAULT_COOLDOWN_MS = 5000L;
	private static final long DEFAULT_DOWN_COOLDOWN_MS = 8000L;
	private static final double DEFAULT_UP_THRESHOLD = 0.50;
	private static final double DEFAULT_DOWN_THRESHOLD = 0.20;
	private static final int DEFAULT_LOW_CHECKS = 3;
	private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

	// Bootstraps Vert.x and starts auto-managed MainVerticle deployments.
	public static void main(String[] args) {
		// Read startup configuration from environment with safe defaults.
		int initialInstances = parsePositiveInt(System.getenv(INITIAL_INSTANCES_ENV), DEFAULT_INITIAL_INSTANCES);
		int maxInstances = parsePositiveInt(System.getenv(MAX_INSTANCES_ENV), DEFAULT_MAX_INSTANCES);
		if (maxInstances < initialInstances) {
			maxInstances = initialInstances;
		}
		int eventLoopThreads = parsePositiveInt(System.getenv(EVENT_LOOP_THREADS_ENV), Math.max(4, maxInstances));
		int targetInFlightPerInstance = parsePositiveInt(System.getenv(TARGET_INFLIGHT_ENV),
				DEFAULT_TARGET_INFLIGHT_PER_INSTANCE);
		long checkMs = parsePositiveLong(System.getenv(AUTOSCALE_CHECK_MS_ENV), DEFAULT_CHECK_MS);
		long cooldownMs = parsePositiveLong(System.getenv(AUTOSCALE_COOLDOWN_MS_ENV), DEFAULT_COOLDOWN_MS);
		long downCooldownMs = parsePositiveLong(System.getenv(AUTOSCALE_DOWN_COOLDOWN_MS_ENV), DEFAULT_DOWN_COOLDOWN_MS);
		double upThreshold = parsePositiveDouble(System.getenv(AUTOSCALE_UP_THRESHOLD_ENV), DEFAULT_UP_THRESHOLD);
		double downThreshold = parsePositiveDouble(System.getenv(AUTOSCALE_DOWN_THRESHOLD_ENV), DEFAULT_DOWN_THRESHOLD);
		int lowChecksRequired = parsePositiveInt(System.getenv(AUTOSCALE_LOW_CHECKS_ENV), DEFAULT_LOW_CHECKS);
		if (downThreshold >= upThreshold) {
			downThreshold = Math.max(0.05, upThreshold * 0.6);
		}
		boolean autoscaleEnabled = parseBoolean(System.getenv(AUTOSCALE_ENABLED_ENV), true);

		int availableProcessors = Runtime.getRuntime().availableProcessors();
		log.info(
				"Launcher config: initialInstances={}, maxInstances={}, eventLoopThreads={}, targetInFlightPerInstance={}, autoscaleEnabled={}, upThreshold={}, downThreshold={}, checkMs={}, upCooldownMs={}, downCooldownMs={}, lowChecksRequired={}, availableProcessors={}, jvmLiveThreads={}",
				initialInstances, maxInstances, eventLoopThreads, targetInFlightPerInstance, autoscaleEnabled,
				upThreshold, downThreshold, checkMs, cooldownMs, downCooldownMs, lowChecksRequired, availableProcessors, THREAD_BEAN.getThreadCount());

		VertxOptions vertxOptions = new VertxOptions()
				.setEventLoopPoolSize(eventLoopThreads);

		Vertx vertx = Vertx.vertx(vertxOptions);
		// Tracks one deployment ID per deployed verticle so scale-down can undeploy a single instance.
		Deque<String> deploymentIds = new ConcurrentLinkedDeque<>();
		AtomicLong lastScaleUpEpochMs = new AtomicLong(0L);
		AtomicLong lastScaleDownEpochMs = new AtomicLong(0L);

		// Start with baseline instances.
		for (int i = 0; i < initialInstances; i++) {
			deploySingleVerticle(vertx, deploymentIds, "initial");
		}
		log.info("Initial deployment requested for {} verticles", initialInstances);

		if (autoscaleEnabled) {
			startAutoscaler(vertx, deploymentIds, initialInstances, maxInstances, targetInFlightPerInstance,
					upThreshold, downThreshold, checkMs, cooldownMs, downCooldownMs, lowChecksRequired,
					lastScaleUpEpochMs, lastScaleDownEpochMs);
		}
	}

	// Periodically evaluates load and applies scale-up / scale-down decisions.
	private static void startAutoscaler(Vertx vertx, Deque<String> deploymentIds, int minInstances, int maxInstances,
			int targetInFlightPerInstance, double upThreshold, double downThreshold, long checkMs, long upCooldownMs,
			long downCooldownMs, int lowChecksRequired, AtomicLong lastScaleUpEpochMs, AtomicLong lastScaleDownEpochMs) {
		// Requires multiple consecutive low-load checks before scaling down.
		final int[] lowUtilizationStreak = {0};
		vertx.setPeriodic(checkMs, ignored -> {
			// Live utilization is derived from in-flight requests and active verticle count.
			int activeInstances = Math.max(1, MainVerticle.getActiveVerticleInstances());
			int inFlight = MainVerticle.getInFlightRequests();
			double utilization = inFlight / (double) (activeInstances * targetInFlightPerInstance);

			log.info(
					"autoscale-check activeInstances={} inFlight={} targetPerInstance={} utilization={} lowUtilizationStreak={}",
					activeInstances, inFlight, targetInFlightPerInstance, String.format("%.2f", utilization),
					lowUtilizationStreak[0]);

			long now = System.currentTimeMillis();
			// Scale up by one verticle when utilization is above threshold and cooldown allows it.
			if (utilization >= upThreshold && activeInstances < maxInstances
					&& now - lastScaleUpEpochMs.get() >= upCooldownMs) {
				lastScaleUpEpochMs.set(now);
				lowUtilizationStreak[0] = 0;
				deploySingleVerticle(vertx, deploymentIds, "autoscale-up");
				return;
			}

			// Track low utilization streak to avoid scale-down flapping.
			if (utilization <= downThreshold) {
				lowUtilizationStreak[0]++;
			} else {
				lowUtilizationStreak[0] = 0;
			}

			// Scale down by undeploying one verticle only when all safety conditions are met.
			boolean canScaleDown = lowUtilizationStreak[0] >= lowChecksRequired
					&& activeInstances > minInstances
					&& deploymentIds.size() > minInstances
					&& now - lastScaleDownEpochMs.get() >= downCooldownMs;
			if (canScaleDown) {
				lowUtilizationStreak[0] = 0;
				lastScaleDownEpochMs.set(now);
				String deploymentId = deploymentIds.pollLast();
				if (deploymentId == null) {
					return;
				}

				vertx.undeploy(deploymentId)
						.onSuccess(v -> log.info(
								"autoscale-down undeployedVerticle=true deploymentId={} activeInstancesAfterScale={} minInstances={}",
								deploymentId, MainVerticle.getActiveVerticleInstances(), minInstances))
						.onFailure(err -> {
							deploymentIds.addLast(deploymentId);
							log.error("autoscale-down failed deploymentId={}", deploymentId, err);
						});
			}
		});
	}

	// Deploys a single MainVerticle and records its deployment id.
	private static void deploySingleVerticle(Vertx vertx, Deque<String> deploymentIds, String reason) {
		vertx.deployVerticle(MainVerticle.class.getName(), new DeploymentOptions().setInstances(1))
				.onSuccess(id -> {
					deploymentIds.add(id);
					log.info("{} deployedNewVerticle=true deploymentId={} activeInstancesNow={}",
							reason, id, MainVerticle.getActiveVerticleInstances());
				})
				.onFailure(err -> log.error("{} failed to deploy verticle", reason, err));
	}

	// Parses positive integer values from env vars and falls back to default on invalid input.
	private static int parsePositiveInt(String value, int defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException ex) {
			return defaultValue;
		}
	}

	// Parses positive long values from env vars and falls back to default on invalid input.
	private static long parsePositiveLong(String value, long defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			long parsed = Long.parseLong(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException ex) {
			return defaultValue;
		}
	}

	// Parses positive double values from env vars and falls back to default on invalid input.
	private static double parsePositiveDouble(String value, double defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			double parsed = Double.parseDouble(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException ex) {
			return defaultValue;
		}
	}

	// Parses boolean values from env vars and falls back to default when missing.
	private static boolean parseBoolean(String value, boolean defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value.trim());
	}
}
