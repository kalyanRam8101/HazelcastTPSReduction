package com.griddynamcics.hazelcasttpsreduction.utils;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Utility class to parse CSV file containing student data.
 * Expected CSV format: Student_Id,cgpa
 */
public class CSVParser {

    private static final Logger log = LoggerFactory.getLogger(CSVParser.class);
    /**
     * Parses a CSV file and returns a list of Student objects.
     *
     * @param filePath Path to the CSV file
     * @return List of Student objects
     */
    public List<Student> parseCSV(String filePath) {
        List<Student> students = new ArrayList<>();
        int successCount = 0;
        int failureCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 0;

            // Skip header row
            br.readLine();
            lineNumber++;

            // Read each line and parse into Student object
            while ((line = br.readLine()) != null) {
                lineNumber++;
                try {
                    Student student = parseStudentLine(line);
                    if (student != null) {
                        students.add(student);
                        successCount++;
                    }
                } catch (Exception e) {
                    failureCount++;
                    log.error("Failed to parse line " + lineNumber + ": " + line + " - Error: " + e.getMessage());
                }
            }

            log.info("CSV parsing completed. Success: " + successCount + ", Failures: " + failureCount);

        } catch (IOException e) {
            log.error("Error reading CSV file: " + filePath + " - " + e.getMessage());
        }

        return students;
    }

    /**
     * Parses a single CSV line into a Student object.
     *
     * @param line CSV line (format: Student_Id,cgpa)
     * @return Student object
     */
    private Student parseStudentLine(String line) {
        String[] fields = line.split(",");

        if (fields.length < 2) {
            throw new IllegalArgumentException("Invalid CSV format. Expected 2 fields (Student_Id, cgpa)");
        }

        String studentId = fields[0].trim();
        String cgpaStr = fields[1].trim();

        if (studentId.isEmpty()) {
            throw new IllegalArgumentException("Student ID cannot be empty");
        }

        Double cgpa;
        try {
            cgpa = Double.parseDouble(cgpaStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid CGPA value: " + cgpaStr);
        }

        return new Student(studentId, cgpa);
    }
}

