package com.griddynamcics.hazelcasttpsreduction;

import com.griddynamcics.hazelcasttpsreduction.verticles.MainVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {
	private static final Logger log = LoggerFactory.getLogger(Launcher.class);

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		Future<String> deployVerticle = vertx.deployVerticle(new MainVerticle());
		deployVerticle
				.onSuccess(id -> log.info("Verticle started successfully, deploymentId={}", id))
				.onFailure(err -> log.error("Failed to start verticle", err));

	}

}
