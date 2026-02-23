package com.griddynamcics.hazelcasttpsreduction.utils;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import io.vertx.core.json.JsonObject;

public class StudentDTO {
	private String id;
	private String name;
	private int age;

	// Default constructor for JSON/object mapping.
	public StudentDTO() {
	}

	// DTO constructor used by mapping helpers.
	public StudentDTO(String id, String name, int age) {
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	// Converts API DTO to internal entity.
	public Student toEntity() {
		return new Student(id, name, age);
	}

	// Converts internal entity to API DTO.
	public static StudentDTO fromEntity(Student student) {
		return new StudentDTO(student.getId(), student.getName(), student.getAge());
	}

	// Converts DTO to JSON response payload.
	public JsonObject toJson() {
		return new JsonObject()
				.put("id", id)
				.put("name", name)
				.put("age", age);
	}

	// Creates DTO from request JSON body.
	public static StudentDTO fromJson(JsonObject json) {
		return new StudentDTO(
				json.getString("id"),
				json.getString("name"),
				json.getInteger("age", 0)
		);
	}
}
