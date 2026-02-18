package com.griddynamcics.hazelcasttpsreduction.service;

import com.google.common.util.concurrent.RateLimiter;
import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import com.griddynamcics.hazelcasttpsreduction.repo.HazelCastRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

public class StudentService {
	private static final Logger log = LoggerFactory.getLogger(StudentService.class);
	private final HazelCastRepository repository;

	public StudentService(HazelCastRepository repository) {
		this.repository = repository;
	}

	public void loadInitialData() {
		if (!repository.isEmpty()) {
			return;
		}

		repository.save(new Student("S-1001", "Alice", 20));
		repository.save(new Student("S-1002", "Bob", 22));
		repository.save(new Student("S-1003", "Carol", 21));
	}

	public List<Student> getAllStudents() {
		return new ArrayList<>(repository.findAll());
	}

	public Optional<Student> getById(String id) {
		return repository.findById(id);
	}

	public Student upsert(Student student) {
		repository.save(student);
		return student;
	}

	public BulkUpsertMetrics upsertBulk(List<Student> students, RateLimiter recordsRateLimiter, int chunkSize,
			IntConsumer progressCallback) {
		if (students.isEmpty()) {
			return new BulkUpsertMetrics(0, 0L, 0L, 0.0);
		}

		long overallStartNanos = System.nanoTime();
		long overallStartEpochMs = System.currentTimeMillis();
		AtomicInteger chunkCounter = new AtomicInteger(1);

		for (int start = 0; start < students.size(); start += chunkSize) {
			int end = Math.min(start + chunkSize, students.size());
			int chunkNumber = chunkCounter.getAndIncrement();
			List<Student> chunk = new ArrayList<>(students.subList(start, end));
			int processed = processStudentChunk(chunk, chunkNumber, recordsRateLimiter);
			if (progressCallback != null) {
				progressCallback.accept(processed);
			}
		}

		long overallDurationNanos = System.nanoTime() - overallStartNanos;
		long overallEndEpochMs = System.currentTimeMillis();
		double overallDurationSeconds = Math.max(overallDurationNanos / 1_000_000_000.0, 0.000001);
		double overallTps = students.size() / overallDurationSeconds;
		return new BulkUpsertMetrics(students.size(), overallStartEpochMs, overallEndEpochMs, overallTps);
	}

	public CsvImportMetrics importCgpaCsv(String csvPath, boolean hasHeader, RateLimiter recordsRateLimiter,
			int chunkSize, IntConsumer progressCallback) throws IOException {
		Path path = Path.of(csvPath);
		if (!Files.exists(path)) {
			throw new IOException("CSV file not found: " + csvPath);
		}

		long overallStartNanos = System.nanoTime();
		long overallStartEpochMs = System.currentTimeMillis();
		AtomicInteger chunkCounter = new AtomicInteger(1);
		AtomicInteger skipped = new AtomicInteger(0);
		int processed = 0;
		Map<String, Double> currentChunk = new HashMap<>(chunkSize);

		try (BufferedReader reader = Files.newBufferedReader(path)) {
			String line;
			boolean skipFirstLine = hasHeader;
			while ((line = reader.readLine()) != null) {
				String trimmed = line.trim();
				if (trimmed.isEmpty()) {
					skipped.incrementAndGet();
					continue;
				}
				if (skipFirstLine) {
					skipFirstLine = false;
					continue;
				}

				Map.Entry<String, Double> parsed = parseCgpaLine(trimmed);
				if (parsed == null) {
					skipped.incrementAndGet();
					continue;
				}

				currentChunk.put(parsed.getKey(), parsed.getValue());
				if (currentChunk.size() >= chunkSize) {
					int chunkProcessed = processCgpaChunk(new HashMap<>(currentChunk), chunkCounter.getAndIncrement(),
							recordsRateLimiter);
					processed += chunkProcessed;
					if (progressCallback != null) {
						progressCallback.accept(chunkProcessed);
					}
					currentChunk.clear();
				}
			}
		}

		if (!currentChunk.isEmpty()) {
			int chunkProcessed = processCgpaChunk(new HashMap<>(currentChunk), chunkCounter.getAndIncrement(),
					recordsRateLimiter);
			processed += chunkProcessed;
			if (progressCallback != null) {
				progressCallback.accept(chunkProcessed);
			}
		}

		long overallDurationNanos = System.nanoTime() - overallStartNanos;
		long overallEndEpochMs = System.currentTimeMillis();
		double overallDurationSeconds = Math.max(overallDurationNanos / 1_000_000_000.0, 0.000001);
		double overallTps = processed / overallDurationSeconds;
		return new CsvImportMetrics(processed, skipped.get(), overallStartEpochMs, overallEndEpochMs, overallTps);
	}

	public Optional<Double> getCgpaByStudentId(String studentId) {
		return repository.findCgpaByStudentId(studentId);
	}

	public long getCgpaCount() {
		return repository.getCgpaCount();
	}

	private int processStudentChunk(List<Student> chunk, int chunkNumber, RateLimiter recordsRateLimiter) {
		String threadName = Thread.currentThread().getName();
		long chunkStartEpochMs = System.currentTimeMillis();
		long chunkStartNanos = System.nanoTime();
		log.info("bulk-chunk-start chunk={} thread={} startTime={} startEpochMs={} records={}",
				chunkNumber, threadName, Instant.ofEpochMilli(chunkStartEpochMs), chunkStartEpochMs, chunk.size());

		recordsRateLimiter.acquire(chunk.size());
		repository.saveAll(chunk);

		long chunkEndEpochMs = System.currentTimeMillis();
		long chunkDurationNanos = System.nanoTime() - chunkStartNanos;
		double chunkDurationSeconds = Math.max(chunkDurationNanos / 1_000_000_000.0, 0.000001);
		double chunkTps = chunk.size() / chunkDurationSeconds;
		log.info("bulk-chunk-end chunk={} thread={} endTime={} endEpochMs={} durationMs={} records={} tps={}",
				chunkNumber, threadName, Instant.ofEpochMilli(chunkEndEpochMs), chunkEndEpochMs,
				chunkDurationNanos / 1_000_000, chunk.size(), String.format("%.2f", chunkTps));
		return chunk.size();
	}

	private int processCgpaChunk(Map<String, Double> chunk, int chunkNumber, RateLimiter recordsRateLimiter) {
		String threadName = Thread.currentThread().getName();
		long chunkStartEpochMs = System.currentTimeMillis();
		long chunkStartNanos = System.nanoTime();
		log.info("csv-chunk-start chunk={} thread={} startTime={} startEpochMs={} records={}",
				chunkNumber, threadName, Instant.ofEpochMilli(chunkStartEpochMs), chunkStartEpochMs, chunk.size());

		recordsRateLimiter.acquire(chunk.size());
		repository.saveCgpaAll(chunk);

		long chunkEndEpochMs = System.currentTimeMillis();
		long chunkDurationNanos = System.nanoTime() - chunkStartNanos;
		double chunkDurationSeconds = Math.max(chunkDurationNanos / 1_000_000_000.0, 0.000001);
		double chunkTps = chunk.size() / chunkDurationSeconds;
		log.info("csv-chunk-end chunk={} thread={} endTime={} endEpochMs={} durationMs={} records={} tps={}",
				chunkNumber, threadName, Instant.ofEpochMilli(chunkEndEpochMs), chunkEndEpochMs,
				chunkDurationNanos / 1_000_000, chunk.size(), String.format("%.2f", chunkTps));
		return chunk.size();
	}

	private Map.Entry<String, Double> parseCgpaLine(String line) {
		String sanitized = line.replace("\uFEFF", "").trim();
		if (sanitized.isEmpty()) {
			return null;
		}

		String studentId = null;
		String cgpaValue = null;

		if (sanitized.contains("=") || sanitized.toLowerCase(Locale.ROOT).contains("student_id")
				|| sanitized.toLowerCase(Locale.ROOT).contains("cgpa")) {
			String[] tokens = sanitized.split("[,;|\\t]");
			for (String token : tokens) {
				String[] kv = token.split("[:=]", 2);
				if (kv.length < 2) {
					continue;
				}
				String key = normalize(kv[0]);
				String value = cleanToken(kv[1]);
				if (key.equals("studentid") || key.equals("student_id")) {
					studentId = value;
				}
				if (key.equals("cgpa")) {
					cgpaValue = value;
				}
			}
		}

		if (studentId == null || cgpaValue == null) {
			String[] parts = sanitized.split("[,;|\\t:]", 2);
			if (parts.length >= 2) {
				studentId = cleanToken(parts[0]);
				cgpaValue = cleanToken(parts[1]);
			}
		}

		if (studentId == null || studentId.isBlank() || cgpaValue == null || cgpaValue.isBlank()) {
			return null;
		}

		try {
			double cgpa = Double.parseDouble(cgpaValue);
			if (!Double.isFinite(cgpa)) {
				return null;
			}
			return new AbstractMap.SimpleEntry<>(studentId, cgpa);
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	private String cleanToken(String value) {
		String cleaned = value.trim();
		if (cleaned.startsWith("\"") && cleaned.endsWith("\"") && cleaned.length() >= 2) {
			cleaned = cleaned.substring(1, cleaned.length() - 1);
		}
		return cleaned.trim();
	}

	private String normalize(String value) {
		return cleanToken(value).toLowerCase(Locale.ROOT).replace(" ", "").replace("-", "").replace(".", "");
	}

	public static class BulkUpsertMetrics {
		private final int processedRecords;
		private final long startEpochMs;
		private final long endEpochMs;
		private final double tps;

		public BulkUpsertMetrics(int processedRecords, long startEpochMs, long endEpochMs, double tps) {
			this.processedRecords = processedRecords;
			this.startEpochMs = startEpochMs;
			this.endEpochMs = endEpochMs;
			this.tps = tps;
		}

		public int getProcessedRecords() {
			return processedRecords;
		}

		public long getStartEpochMs() {
			return startEpochMs;
		}

		public long getEndEpochMs() {
			return endEpochMs;
		}

		public double getTps() {
			return tps;
		}
	}

	public static class CsvImportMetrics {
		private final int processedRecords;
		private final int skippedRecords;
		private final long startEpochMs;
		private final long endEpochMs;
		private final double tps;

		public CsvImportMetrics(int processedRecords, int skippedRecords, long startEpochMs, long endEpochMs, double tps) {
			this.processedRecords = processedRecords;
			this.skippedRecords = skippedRecords;
			this.startEpochMs = startEpochMs;
			this.endEpochMs = endEpochMs;
			this.tps = tps;
		}

		public int getProcessedRecords() {
			return processedRecords;
		}

		public int getSkippedRecords() {
			return skippedRecords;
		}

		public long getStartEpochMs() {
			return startEpochMs;
		}

		public long getEndEpochMs() {
			return endEpochMs;
		}

		public double getTps() {
			return tps;
		}
	}
}
