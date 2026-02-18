package com.griddynamcics.hazelcasttpsreduction.verticles;

import com.google.common.util.concurrent.RateLimiter;
import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import com.griddynamcics.hazelcasttpsreduction.repo.HazelCastRepository;
import com.griddynamcics.hazelcasttpsreduction.repo.HazelCastRepositoryImpl;
import com.griddynamcics.hazelcasttpsreduction.service.StudentService;
import com.griddynamcics.hazelcasttpsreduction.utils.StudentDTO;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {
	private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);
	private static final int PORT = 8888;
	private static final double REQUESTS_PER_SECOND = 1000.0;
	private static final double RECORDS_PER_SECOND = 100000.0;
	private static final int MAX_RECORDS_PER_REQUEST = 100000;
	private static final int WORKER_POOL_SIZE = 16;
	private static final int BULK_CHUNK_SIZE = 1000;
	private static final AtomicInteger ACTIVE_VERTICLE_INSTANCES = new AtomicInteger(0);
	private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

	private final RateLimiter requestRateLimiter = RateLimiter.create(REQUESTS_PER_SECOND);
	private final RateLimiter recordsRateLimiter = RateLimiter.create(RECORDS_PER_SECOND);
	private WorkerExecutor workerExecutor;
	private HazelCastRepository repository;
	private StudentService studentService;
	private boolean serverStarted;

	@Override
	public void start(Promise<Void> startPromise) {
		workerExecutor = vertx.createSharedWorkerExecutor("student-worker-pool", WORKER_POOL_SIZE);
		vertx.<Void>executeBlocking(promise -> {
			repository = new HazelCastRepositoryImpl();
			studentService = new StudentService(repository);
			studentService.loadInitialData();
			promise.complete();
		}).onSuccess(ignored -> startHttpServer(startPromise))
				.onFailure(error -> {
					startPromise.fail(error);
					log.error("Failed to initialize services", error);
				});
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		vertx.<Void>executeBlocking(promise -> {
			if (repository != null) {
				repository.shutdown();
			}
			promise.complete();
		}).onComplete(ignored -> {
			if (workerExecutor != null) {
				workerExecutor.close();
			}
			int activeInstances = serverStarted ? ACTIVE_VERTICLE_INSTANCES.decrementAndGet()
					: ACTIVE_VERTICLE_INSTANCES.get();
			log.info("Verticle stopped, deploymentId={}, activeVerticleInstances={}, jvmLiveThreads={}",
					deploymentID(), Math.max(activeInstances, 0), THREAD_BEAN.getThreadCount());
			logRuntimeStats("shutdown");
			stopPromise.complete();
		});
	}

	private void startHttpServer(Promise<Void> startPromise) {
		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());
		router.route().handler(this::requestRateLimit);

		router.get("/health").handler(this::health);
		router.get("/students").handler(this::getStudents);
		router.get("/students/:id").handler(this::getStudentById);
		router.post("/students").handler(this::upsertStudent);
		router.post("/students/bulk").handler(this::upsertStudentsBulk);
		router.post("/cgpa/import").handler(this::importCgpaCsv);
		router.get("/cgpa/count").handler(this::getCgpaCount);
		router.get("/cgpa/:studentId").handler(this::getCgpaByStudentId);

		HttpServerOptions serverOptions = new HttpServerOptions()
				.setReusePort(true);

		vertx.createHttpServer(serverOptions)
				.requestHandler(router)
				.listen(PORT)
				.onSuccess(http -> {
					serverStarted = true;
					startPromise.complete();
					int activeInstances = ACTIVE_VERTICLE_INSTANCES.incrementAndGet();
					log.info(
							"HTTP server started on port {}, deploymentId={}, activeVerticleInstances={}, jvmLiveThreads={}, workerPoolSizeConfigured={}",
							PORT, deploymentID(), activeInstances, THREAD_BEAN.getThreadCount(), WORKER_POOL_SIZE);
					logRuntimeStats("startup");
				})
				.onFailure(error -> {
					startPromise.fail(error);
					log.error("Failed to start HTTP serve100
										 
						
				
					
						 											
					
					 
				
							
							
							
				", error);
				});
	}

	private void requestRateLimit(RoutingContext context) {
		if (requestRateLimiter.tryAcquire()) {
			context.next();
			return;
		}

		JsonObject errorBody = new JsonObject()
				.put("error", "Too Many Requests")
				.put("message", "Request rate limit exceeded. Try again shortly.");

		context.response()
				.setStatusCode(429)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.putHeader(HttpHeaders.RETRY_AFTER, "1")
				.end(errorBody.encode());
	}

	private void health(RoutingContext context) {
		context.response()
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(new JsonObject()
						.put("status", "UP")
						.put("recordsRateLimitPerSecond", (int) RECORDS_PER_SECOND)
						.put("maxRecordsPerRequest", MAX_RECORDS_PER_REQUEST)
						.put("activeVerticleInstances", ACTIVE_VERTICLE_INSTANCES.get())
						.put("jvmLiveThreads", THREAD_BEAN.getThreadCount())
						.put("workerPoolConfigured", WORKER_POOL_SIZE)
						.encode());
	}

	private void logRuntimeStats(String stage) {
		log.info(
				"runtime-stats stage={} deploymentId={} activeVerticleInstances={} jvmLiveThreads={} workerPoolConfigured={}",
				stage, deploymentID(), ACTIVE_VERTICLE_INSTANCES.get(), THREAD_BEAN.getThreadCount(), WORKER_POOL_SIZE);
	}

	private void getStudents(RoutingContext context) {
		workerExecutor.<JsonArray>executeBlocking(promise -> {
			List<StudentDTO> students = studentService.getAllStudents().stream()
					.map(StudentDTO::fromEntity)
					.collect(Collectors.toList());
			promise.complete(new JsonArray(students.stream().map(StudentDTO::toJson).collect(Collectors.toList())));
		}).onSuccess(response -> context.response()
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(response.encode()))
				.onFailure(error -> internalError(context, error));
	}

	private void getStudentById(RoutingContext context) {
		String id = context.pathParam("id");
		workerExecutor.<StudentDTO>executeBlocking(promise -> {
			StudentDTO dto = studentService.getById(id).map(StudentDTO::fromEntity).orElse(null);
			promise.complete(dto);
		}).onSuccess(dto -> {
			if (dto == null) {
				context.response()
						.setStatusCode(404)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(new JsonObject().put("error", "Student not found").put("id", id).encode());
				return;
			}

			context.response()
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(dto.toJson().encode());
		}).onFailure(error -> internalError(context, error));
	}

	private void upsertStudent(RoutingContext context) {
		JsonObject body = context.body().asJsonObject();
		if (body == null) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "Request body must be JSON object").encode());
			return;
		}

		StudentDTO dto = StudentDTO.fromJson(body);
		if (isInvalid(dto)) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "id, name and age (>0) are required").encode());
			return;
		}

		workerExecutor.<Student>executeBlocking(promise -> promise.complete(studentService.upsert(dto.toEntity())))
				.onSuccess(student -> context.response()
						.setStatusCode(201)
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(StudentDTO.fromEntity(student).toJson().encode()))
				.onFailure(error -> internalError(context, error));
	}

	private void upsertStudentsBulk(RoutingContext context) {
		List<StudentDTO> students = parseStudents(context.body().asJsonArray(), context.body().asJsonObject());
		if (students == null) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "Body must be a JSON array or object with 'students' array")
							.encode());
			return;
		}
		if (students.isEmpty()) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "students list cannot be empty").encode());
			return;
		}
		if (students.size() > MAX_RECORDS_PER_REQUEST) {
			context.response()
					.setStatusCode(413)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "Max records per request is " + MAX_RECORDS_PER_REQUEST)
							.encode());
			return;
		}
		if (students.stream().anyMatch(this::isInvalid)) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "All records must contain valid id, name and age (>0)")
							.encode());
			return;
		}

		List<Student> entities = students.stream().map(StudentDTO::toEntity).collect(Collectors.toList());
		String requestId = UUID.randomUUID().toString();
		long requestStartEpochMs = System.currentTimeMillis();
		LiveTpsTracker liveTpsTracker = new LiveTpsTracker();
		log.info(
				"bulk-request-start requestId={} thread={} startTime={} startEpochMs={} requestedRecords={} configuredRecordsPerSec={}",
				requestId, Thread.currentThread().getName(), Instant.ofEpochMilli(requestStartEpochMs),
				requestStartEpochMs, entities.size(), (int) RECORDS_PER_SECOND);

		workerExecutor.<StudentService.BulkUpsertMetrics>executeBlocking(promise -> {
			StudentService.BulkUpsertMetrics metrics = studentService.upsertBulk(entities, recordsRateLimiter,
					BULK_CHUNK_SIZE, processedChunk -> {
						double liveTps = liveTpsTracker.onProcessed(processedChunk);
						log.info("bulk-live-tps requestId={} processedSoFar={} liveTps={}",
								requestId, liveTpsTracker.getProcessed(), String.format("%.2f", liveTps));
					});
			promise.complete(metrics);
		}).onSuccess(metrics -> {
			long requestEndEpochMs = System.currentTimeMillis();
			long requestDurationMs = requestEndEpochMs - requestStartEpochMs;
			log.info(
					"bulk-request-end requestId={} thread={} endTime={} endEpochMs={} durationMs={} processedRecords={} tps={}",
					requestId, Thread.currentThread().getName(), Instant.ofEpochMilli(requestEndEpochMs),
					requestEndEpochMs, requestDurationMs, metrics.getProcessedRecords(),
					String.format("%.2f", metrics.getTps()));

			context.response()
					.setStatusCode(202)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject()
							.put("requestId", requestId)
							.put("processed", metrics.getProcessedRecords())
							.put("startEpochMs", metrics.getStartEpochMs())
							.put("endEpochMs", metrics.getEndEpochMs())
							.put("tps", Math.round(metrics.getTps() * 100.0) / 100.0)
							.put("liveTps", Math.round(liveTpsTracker.getCurrentTps() * 100.0) / 100.0)
							.put("recordsRateLimitPerSecond", (int) RECORDS_PER_SECOND)
							.put("chunkSize", BULK_CHUNK_SIZE)
							.encode());
		})
				.onFailure(error -> internalError(context, error));
	}

	private void importCgpaCsv(RoutingContext context) {
		JsonObject body = context.body().asJsonObject();
		if (body == null) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "Request body must be JSON object").encode());
			return;
		}

		String csvPath = body.getString("csvPath");
		boolean hasHeader = body.getBoolean("hasHeader", true);
		if (csvPath == null || csvPath.isBlank()) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "csvPath is required").encode());
			return;
		}

		String requestId = UUID.randomUUID().toString();
		long requestStartEpochMs = System.currentTimeMillis();
		LiveTpsTracker liveTpsTracker = new LiveTpsTracker();
		log.info("csv-import-start requestId={} thread={} startTime={} startEpochMs={} csvPath={} hasHeader={}",
				requestId, Thread.currentThread().getName(), Instant.ofEpochMilli(requestStartEpochMs),
				requestStartEpochMs, csvPath, hasHeader);

		workerExecutor.<StudentService.CsvImportMetrics>executeBlocking(promise -> {
			try {
				StudentService.CsvImportMetrics metrics = studentService.importCgpaCsv(csvPath, hasHeader,
						recordsRateLimiter, BULK_CHUNK_SIZE, processedChunk -> {
							double liveTps = liveTpsTracker.onProcessed(processedChunk);
							log.info("csv-live-tps requestId={} processedSoFar={} liveTps={}",
									requestId, liveTpsTracker.getProcessed(), String.format("%.2f", liveTps));
						});
				promise.complete(metrics);
			} catch (Exception ex) {
				promise.fail(ex);
			}
		}).onSuccess(metrics -> {
			long requestEndEpochMs = System.currentTimeMillis();
			long durationMs = requestEndEpochMs - requestStartEpochMs;
			log.info(
					"csv-import-end requestId={} thread={} endTime={} endEpochMs={} durationMs={} processedRecords={} skippedRecords={} tps={}",
					requestId, Thread.currentThread().getName(), Instant.ofEpochMilli(requestEndEpochMs),
					requestEndEpochMs, durationMs, metrics.getProcessedRecords(), metrics.getSkippedRecords(),
					String.format("%.2f", metrics.getTps()));

			context.response()
					.setStatusCode(202)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject()
							.put("requestId", requestId)
							.put("processed", metrics.getProcessedRecords())
							.put("skipped", metrics.getSkippedRecords())
							.put("startEpochMs", metrics.getStartEpochMs())
							.put("endEpochMs", metrics.getEndEpochMs())
							.put("tps", Math.round(metrics.getTps() * 100.0) / 100.0)
							.put("liveTps", Math.round(liveTpsTracker.getCurrentTps() * 100.0) / 100.0)
							.put("recordsRateLimitPerSecond", (int) RECORDS_PER_SECOND)
							.put("chunkSize", BULK_CHUNK_SIZE)
							.put("cgpaCountInHazelcast", studentService.getCgpaCount())
							.encode());
		}).onFailure(error -> {
			log.error("csv-import-failed requestId={} message={}", requestId, error.getMessage(), error);
			context.response()
					.setStatusCode(500)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", error.getMessage()).encode());
		});
	}

	private void getCgpaByStudentId(RoutingContext context) {
		String studentId = context.pathParam("studentId");
		if (studentId == null || studentId.isBlank()) {
			context.response()
					.setStatusCode(400)
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(new JsonObject().put("error", "studentId is required").encode());
			return;
		}

		workerExecutor
				.<Double>executeBlocking(
						promise -> promise.complete(studentService.getCgpaByStudentId(studentId).orElse(null)))
				.onSuccess(cgpa -> {
					if (cgpa == null) {
						context.response()
								.setStatusCode(404)
								.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
								.end(new JsonObject().put("error", "CGPA not found").put("studentId", studentId)
										.encode());
						return;
					}
					context.response()
							.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
							.end(new JsonObject().put("studentId", studentId).put("cgpa", cgpa).encode());
				})
				.onFailure(error -> internalError(context, error));
	}

	private void getCgpaCount(RoutingContext context) {
		workerExecutor.<Long>executeBlocking(promise -> promise.complete(studentService.getCgpaCount()))
				.onSuccess(count -> context.response()
						.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
						.end(new JsonObject().put("cgpaCount", count).encode()))
				.onFailure(error -> internalError(context, error));
	}

	private List<StudentDTO> parseStudents(JsonArray bodyArray, JsonObject bodyObject) {
		JsonArray studentsArray = bodyArray;
		if (studentsArray == null && bodyObject != null) {
			studentsArray = bodyObject.getJsonArray("students");
		}
		if (studentsArray == null) {
			return null;
		}

		List<StudentDTO> list = new ArrayList<>(studentsArray.size());
		for (int i = 0; i < studentsArray.size(); i++) {
			Object item = studentsArray.getValue(i);
			if (!(item instanceof JsonObject json)) {
				return null;
			}
			list.add(StudentDTO.fromJson(json));
		}
		return list;
	}

	private void internalError(RoutingContext context, Throwable error) {
		log.error("Request failed", error);
		context.response()
				.setStatusCode(500)
				.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
				.end(new JsonObject().put("error", "Internal Server Error").encode());
	}

	private boolean isInvalid(StudentDTO dto) {
		return dto.getAge() <= 0
				|| Objects.isNull(dto.getId())
				|| dto.getId().isBlank()
				|| Objects.isNull(dto.getName())
				|| dto.getName().isBlank();
	}

	private static class LiveTpsTracker {
		private final long startNanos = System.nanoTime();
		private final AtomicLong processed = new AtomicLong(0);
		private volatile double currentTps = 0.0;

		double onProcessed(long delta) {
			long totalProcessed = processed.addAndGet(delta);
			double elapsedSeconds = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.000001);
			currentTps = totalProcessed / elapsedSeconds;
			return currentTps;
		}

		long getProcessed() {
			return processed.get();
		}

		double getCurrentTps() {
			return currentTps;
		}
	}
}
