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

public class Launcher {
	private static final Logger log = LoggerFactory.getLogger(Launcher.class);
	private static final String INSTANCES_ENV = "MAIN_VERTICLE_INSTANCES";
	private static final String EVENT_LOOP_THREADS_ENV = "VERTX_EVENT_LOOP_THREADS";
	private static final int DEFAULT_INSTANCES = Math.max(2, Runtime.getRuntime().availableProcessors());
	private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

	public static void main(String[] args) {
		int instances = parsePositiveInt(System.getenv(INSTANCES_ENV), DEFAULT_INSTANCES);
		int eventLoopThreads = parsePositiveInt(System.getenv(EVENT_LOOP_THREADS_ENV), Math.max(4, instances));
		int availableProcessors = Runtime.getRuntime().availableProcessors();
		log.info("Launcher config: requestedInstances={}, eventLoopThreads={}, availableProcessors={}, jvmLiveThreads={}",
				instances, eventLoopThreads, availableProcessors, THREAD_BEAN.getThreadCount());

		VertxOptions vertxOptions = new VertxOptions()
				.setEventLoopPoolSize(eventLoopThreads);

		Vertx vertx = Vertx.vertx(vertxOptions);
		DeploymentOptions deploymentOptions = new DeploymentOptions()
				.setInstances(instances);

		Future<String> deployVerticle = vertx.deployVerticle(MainVerticle.class.getName(), deploymentOptions);
		deployVerticle
				.onSuccess(id -> log.info("Verticles deployed successfully, deploymentId={}, instances={}, eventLoopThreads={}",
						id, instances, eventLoopThreads))
				.onFailure(err -> log.error("Failed to start verticle", err));
	}

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
}
