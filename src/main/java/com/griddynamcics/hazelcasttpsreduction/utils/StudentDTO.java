package com.griddynamcics.hazelcasttpsreduction.utils;

import com.griddynamcics.hazelcasttpsreduction.entity.Student;
import io.vertx.core.json.JsonObject;

public class StudentDTO {
	private String id;
	private String name;
	private int age;

	public StudentDTO() {
	}

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

	public Student toEntity() {
		return new Student(id, name, age);
	}

	public static StudentDTO fromEntity(Student student) {
		return new StudentDTO(student.getId(), student.getName(), student.getAge());
	}

	public JsonObject toJson() {
		return new JsonObject()
				.put("id", id)
				.put("name", name)
				.put("age", age);
	}

	public static StudentDTO fromJson(JsonObject json) {
		return new StudentDTO(
				json.getString("id"),
				json.getString("name"),
				json.getInteger("age", 0)
		);
	}
}
