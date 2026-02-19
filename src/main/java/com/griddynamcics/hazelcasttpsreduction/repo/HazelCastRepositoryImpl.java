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
	private static final Object LOCK = new Object();
	private static final AtomicInteger CLIENT_COUNT = new AtomicInteger(0);
	private static HazelcastInstance sharedHazelcastInstance;

	private final HazelcastInstance hazelcastInstance;
	private final IMap<String, Student> students;
	private final IMap<String, Double> studentCgpa;
	private boolean closed;

	public HazelCastRepositoryImpl() {
		this.hazelcastInstance = getOrCreateSharedInstance();
		this.students = hazelcastInstance.getMap(STUDENTS_MAP);
		this.studentCgpa = hazelcastInstance.getMap(STUDENT_CGPA_MAP);
		log.info("Hazelcast repository initialized, activeRepositoryClients={}", CLIENT_COUNT.get());
	}

	@Override
	public void save(Student student) {
		students.put(student.getId(), student);
	}

	@Override
	public void saveAll(Collection<Student> students) {
		Map<String, Student> batch = students.stream()
				.collect(Collectors.toMap(Student::getId, student -> student, (left, right) -> right));
		this.students.putAll(batch);
	}

	@Override
	public Optional<Student> findById(String id) {
		return Optional.ofNullable(students.get(id));
	}

	@Override
	public Collection<Student> findAll() {
		return students.values();
	}

	@Override
	public boolean isEmpty() {
		return students.isEmpty();
	}

	@Override
	public void saveCgpaAll(Map<String, Double> cgpaBatch) {
		studentCgpa.putAll(cgpaBatch);
	}

	@Override
	public Optional<Double> findCgpaByStudentId(String studentId) {
		return Optional.ofNullable(studentCgpa.get(studentId));
	}

	@Override
	public long getCgpaCount() {
		return studentCgpa.size();
	}

	@Override
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

	private Config buildConfig() {
		Config config = new Config();
		config.setClusterName("hazelcast-tps-reduction");
		MapConfig mapConfig = new MapConfig(STUDENTS_MAP);
		mapConfig.setBackupCount(1);
		config.addMapConfig(mapConfig);
		MapConfig cgpaMapConfig = new MapConfig(STUDENT_CGPA_MAP);
		cgpaMapConfig.setBackupCount(1);
		config.addMapConfig(cgpaMapConfig);
		return config;
	}

}
