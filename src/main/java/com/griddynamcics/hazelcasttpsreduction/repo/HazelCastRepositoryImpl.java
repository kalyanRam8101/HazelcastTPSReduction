package com.griddynamcics.hazelcasttpsreduction.repo;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HazelCastRepositoryImpl implements HazelCastRepository {
	private static final Logger log = LoggerFactory.getLogger(HazelCastRepositoryImpl.class);
	private static final String STUDENTS_MAP = "students";
	private static final String STUDENT_CGPA_MAP = "student_cgpa";
	private static final String STUDENTS_TTL_ENV = "STUDENTS_TTL_SECONDS";
	private static final String STUDENT_CGPA_TTL_ENV = "STUDENT_CGPA_TTL_SECONDS";
	private static final int DEFAULT_STUDENTS_TTL_SECONDS = 3600;
	private static final int DEFAULT_STUDENT_CGPA_TTL_SECONDS = 3600;
	private static final Object LOCK = new Object();
	private static final AtomicInteger CLIENT_COUNT = new AtomicInteger(0);
	private static HazelcastInstance sharedHazelcastInstance;

	private final HazelcastInstance hazelcastInstance;
	private final IMap<String, Student> students;
	private final IMap<String, Double> studentCgpa;
	private boolean closed;

	// Reuses a shared Hazelcast member for all repository objects in the same JVM.
	public HazelCastRepositoryImpl() {
		this.hazelcastInstance = getOrCreateSharedInstance();
		this.students = hazelcastInstance.getMap(STUDENTS_MAP);
		this.studentCgpa = hazelcastInstance.getMap(STUDENT_CGPA_MAP);
		log.info("Hazelcast repository initialized, activeRepositoryClients={}", CLIENT_COUNT.get());
	}

	@Override
	// Writes/updates one student.
	public void save(Student student) {
		students.put(student.getId(), student);
	}

	@Override
	// Bulk writes student records in one Hazelcast map operation.
	public void saveAll(Collection<Student> students) {
		Map<String, Student> batch = students.stream()
				.collect(Collectors.toMap(Student::getId, student -> student, (left, right) -> right));
		this.students.putAll(batch);
	}

	@Override
	// Reads one student.
	public Optional<Student> findById(String id) {
		return Optional.ofNullable(students.get(id));
	}

	@Override
	// Reads all students.
	public Collection<Student> findAll() {
		return students.values();
	}

	@Override
	// Checks whether student map is empty.
	public boolean isEmpty() {
		return students.isEmpty();
	}

	@Override
	// Bulk writes cgpa key-value map.
	public void saveCgpaAll(Map<String, Double> cgpaBatch) {
		studentCgpa.putAll(cgpaBatch);
	}

	@Override
	// Reads cgpa by student id.
	public Optional<Double> findCgpaByStudentId(String studentId) {
		return Optional.ofNullable(studentCgpa.get(studentId));
	}

	@Override
	// Returns total number of cgpa records.
	public long getCgpaCount() {
		return studentCgpa.size();
	}

	@Override
	// Ref-count based shutdown to avoid stopping shared Hazelcast while still in use.
	public void shutdown() {
		synchronized (LOCK) {
			if (closed) {
				return;
			}
			closed = true;
			int remainingClients = CLIENT_COUNT.decrementAndGet();
			if (remainingClients <= 0 && sharedHazelcastInstance != null) {
				log.info("Shutting down shared Hazelcast instance");
				sharedHazelcastInstance.shutdown();
				sharedHazelcastInstance = null;
				CLIENT_COUNT.set(0);
				return;
			}
			log.info("Skipping Hazelcast shutdown, activeRepositoryClients={}", Math.max(remainingClients, 0));
		}
	}

	// Creates one shared Hazelcast member on first access.
	private HazelcastInstance getOrCreateSharedInstance() {
		synchronized (LOCK) {
			if (sharedHazelcastInstance == null || !sharedHazelcastInstance.getLifecycleService().isRunning()) {
				log.info("Creating shared Hazelcast instance");
				sharedHazelcastInstance = Hazelcast.newHazelcastInstance(buildConfig());
			}
			CLIENT_COUNT.incrementAndGet();
			return sharedHazelcastInstance;
		}
	}

	// Central Hazelcast map configuration.
	private Config buildConfig() {
		int studentsTtlSeconds = parseNonNegativeInt(
				System.getenv(STUDENTS_TTL_ENV), DEFAULT_STUDENTS_TTL_SECONDS);
		int cgpaTtlSeconds = parseNonNegativeInt(
				System.getenv(STUDENT_CGPA_TTL_ENV), DEFAULT_STUDENT_CGPA_TTL_SECONDS);

		Config config = new Config();
		config.setClusterName("hazelcast-tps-reduction");

		MapConfig mapConfig = new MapConfig(STUDENTS_MAP);
		mapConfig.setBackupCount(1);
		mapConfig.setTimeToLiveSeconds(studentsTtlSeconds);
		config.addMapConfig(mapConfig);

		MapConfig cgpaMapConfig = new MapConfig(STUDENT_CGPA_MAP);
		cgpaMapConfig.setBackupCount(1);
		cgpaMapConfig.setTimeToLiveSeconds(cgpaTtlSeconds);
		config.addMapConfig(cgpaMapConfig);

		log.info("Hazelcast TTL config: studentsTtlSeconds={}, studentCgpaTtlSeconds={}",
				studentsTtlSeconds, cgpaTtlSeconds);
		return config;
	}

	private int parseNonNegativeInt(String value, int defaultValue) {
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed >= 0 ? parsed : defaultValue;
		} catch (NumberFormatException ex) {
			return defaultValue;
		}
	}

}
