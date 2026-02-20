FROM maven:3.8.8-eclipse-temurin-17 AS build
WORKDIR /app

# copy maven config and sources
COPY pom.xml ./
COPY src ./src

# build shaded (fat) jar
RUN mvn -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app

# copy fat jar produced by maven-shade
COPY --from=build /app/target/starter-1.0.0-SNAPSHOT-fat.jar /app/app.jar

EXPOSE 8888

ENTRYPOINT ["java", "-jar", "/app/app.jar", "run", "com.griddynamcics.hazelcasttpsreduction.verticles.MainVerticle"]
