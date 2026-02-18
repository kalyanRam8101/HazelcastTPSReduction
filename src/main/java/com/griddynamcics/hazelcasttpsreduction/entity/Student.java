package com.griddynamcics.hazelcasttpsreduction.entity;

import java.io.Serializable;

public class Student implements Serializable {
    private static final long serialVersionUID = 1L;

    private String studentId;
    private Double cgpa;

    // Default constructor (required for deserialization)
    public Student() {}

    // Parameterized constructor
    public Student(String studentId, Double cgpa) {
        this.studentId = studentId;
        this.cgpa = cgpa;
    }

    // Getters and Setters
    public String getStudentId() {
        return studentId;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public Double getCgpa() {
        return cgpa;
    }

    public void setCgpa(Double cgpa) {
        this.cgpa = cgpa;
    }

    @Override
    public String toString() {
        return "Student{" +
                "studentId='" + studentId + '\'' +
                ", cgpa=" + cgpa +
                '}';
    }
}
