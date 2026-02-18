package com.griddynamcics.hazelcasttpsreduction.service;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import com.griddynamcics.hazelcasttpsreduction.utils.CSVParser;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Service to load data from CSV into Hazelcast IMap.
 */
public class DataLoaderService {

    private static final Logger log = LoggerFactory.getLogger(DataLoaderService.class);

    private final HazelcastInstance hazelcastInstance;
    private final CSVParser csvParser;
    private static final String STUDENT_MAP_NAME = "students";

    public DataLoaderService(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.csvParser = new CSVParser();
    }

    /**
     * Loads student data from CSV file into Hazelcast IMap.
     *
     * @param filePath Path to the CSV file
     * @return Number of students loaded successfully
     */
    public int loadStudentsFromCSV(String filePath) {
        long startTime = System.currentTimeMillis();
        log.info("Starting data load from CSV: " + filePath);

        // Step 1: Parse CSV file
        List<Student> students = csvParser.parseCSV(filePath);

        // Step 2: Get Hazelcast IMap
        IMap<String, Student> studentMap = hazelcastInstance.getMap(STUDENT_MAP_NAME);

        // Step 3: Insert each student into the map
        int loadedCount = 0;
        for (Student student : students) {
            try {
                studentMap.put(student.getStudentId(), student);
                loadedCount++;
            } catch (Exception e) {
                log.error("Failed to insert student " + student.getStudentId() + ": " + e.getMessage());
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("Data load completed. Loaded " + loadedCount + " students into Hazelcast in " + duration + " ms");

        return loadedCount;
    }

    /**
     * Gets the Hazelcast IMap containing student data.
     *
     * @return IMap of students
     */
    public IMap<String, Student> getStudentMap() {
        return hazelcastInstance.getMap(STUDENT_MAP_NAME);
    }

    /**
     * Gets a student by ID from Hazelcast.
     *
     * @param studentId Student ID
     * @return Student object or null if not found
     */
    public Student getStudentById(String studentId) {
        IMap<String, Student> studentMap = hazelcastInstance.getMap(STUDENT_MAP_NAME);
        return studentMap.get(studentId);
    }

    /**
     * Gets the count of students in Hazelcast.
     *
     * @return Number of students in the map
     */
    public int getStudentCount() {
        IMap<String, Student> studentMap = hazelcastInstance.getMap(STUDENT_MAP_NAME);
        return studentMap.size();
    }
}

