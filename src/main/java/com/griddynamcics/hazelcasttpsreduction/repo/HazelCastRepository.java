package com.griddynamcics.hazelcasttpsreduction.repo;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public interface HazelCastRepository {
	void save(Student student);

	void saveAll(Collection<Student> students);

	Optional<Student> findById(String id);

	Collection<Student> findAll();

	boolean isEmpty();

	void saveCgpaAll(Map<String, Double> cgpaBatch);

	Optional<Double> findCgpaByStudentId(String studentId);

	long getCgpaCount();

	void shutdown();
}
