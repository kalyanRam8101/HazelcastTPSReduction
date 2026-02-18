package com.griddynamcics.hazelcasttpsreduction.verticles;

import com.griddynamcics.hazelcasttpsreduction.service.DataLoaderService;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {
	private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);

	private HazelcastInstance hazelcastInstance;
	private DataLoaderService dataLoaderService;

	// Default CSV file path (can be configured)
	private static final String CSV_FILE_PATH = "src/main/resources/students.csv";

	@Override
	public void start(Promise<Void> startPromise) {
		// Step 1: Initialize Hazelcast
		log.info("Initializing Hazelcast instance...");
		hazelcastInstance = Hazelcast.newHazelcastInstance();
		log.info("Hazelcast instance created successfully");

		// Step 2: Initialize DataLoaderService
		dataLoaderService = new DataLoaderService(hazelcastInstance);

		// Step 3: Create Router with API endpoints
		Router router = Router.router(vertx);

		// Health check endpoint
		router.get("/health").handler(this::healthCheck);

		// Load data from CSV endpoint
		router.post("/load-data").handler(this::loadData);

		// Get student count endpoint
		router.get("/students/count").handler(this::getStudentCount);

		// Step 4: Start HTTP server with router
		vertx.createHttpServer()
			.requestHandler(router)
			.listen(8888)
			.onComplete(http -> {
				if (http.succeeded()) {
					startPromise.complete();
					log.info("HTTP server started on port {}", 8888);
				} else {
					startPromise.fail(http.cause());
					log.error("Failed to start HTTP server", http.cause());
				}
			});
	}

	/**
	 * Health check endpoint
	 */
	private void healthCheck(RoutingContext ctx) {
		ctx.response()
			.putHeader("content-type", "application/json")
			.end(new JsonObject().put("status", "UP").encode());
	}

	/**
	 * Load data from CSV into Hazelcast
	 */
	private void loadData(RoutingContext ctx) {
		try {
			int loadedCount = dataLoaderService.loadStudentsFromCSV(CSV_FILE_PATH);
			ctx.response()
				.putHeader("content-type", "application/json")
				.end(new JsonObject()
					.put("status", "success")
					.put("loaded", loadedCount)
					.encode());
		} catch (Exception e) {
			log.error("Failed to load data", e);
			ctx.response()
				.setStatusCode(500)
				.putHeader("content-type", "application/json")
				.end(new JsonObject()
					.put("status", "error")
					.put("message", e.getMessage())
					.encode());
		}
	}

	/**
	 * Get total student count from Hazelcast
	 */
	private void getStudentCount(RoutingContext ctx) {
		int count = dataLoaderService.getStudentCount();
		ctx.response()
			.putHeader("content-type", "application/json")
			.end(new JsonObject().put("count", count).encode());
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		// Shutdown Hazelcast when verticle stops
		if (hazelcastInstance != null) {
			log.info("Shutting down Hazelcast instance...");
			hazelcastInstance.shutdown();
			log.info("Hazelcast instance shut down successfully");
		}
		stopPromise.complete();
	}
}
