package com.griddynamcics.hazelcasttpsreduction;

import com.griddynamcics.hazelcasttpsreduction.verticles.MainVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class Launcher {

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		Future<String> deployVerticle = vertx.deployVerticle(new MainVerticle());
		System.out.println("verticle is started !!" + deployVerticle);

	}

}
