import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  kotlin("jvm") version "2.0.21"
  id("jacoco")
  id("com.github.johnrengelman.shadow") version "8.1.1"
  id("org.barfuin.gradle.jacocolog") version "3.1.0"
  id("org.owasp.dependencycheck")  version "12.1.3"
}

configurations {
  testImplementation { exclude(group = "org.junit.vintage") }
}

dependencies {
  implementation("com.amazonaws:aws-lambda-java-core:1.2.3")
  implementation("software.amazon.awssdk:sfn:2.32.21")
  implementation("software.amazon.awssdk:s3:2.31.62")
  implementation("org.apache.parquet:parquet-avro:1.15.2")
  implementation("com.jsoizo:kotlin-csv-jvm:1.10.0")
  // We need hadoop in implementation scope since it won't be provided in a lambda
  // It is required for parquet-avro to use Avro schemas to convert to Parquet
  implementation("org.apache.hadoop:hadoop-common:3.3.6")


  //test
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.1")
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.1")
  testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.1")
  testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
  testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.1")
  // Hadoop MapReduce is required in Tets scope for converting parquet bytes back to Avro records
  testImplementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")
}
java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

kotlin {
  jvmToolchain(21)
}

tasks {
  withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions.jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
  }
}
repositories {
  mavenLocal()
  mavenCentral()
}
tasks.test {
  finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
  dependsOn(tasks.test)
}

java.sourceCompatibility = JavaVersion.VERSION_21

tasks.jar {
  enabled = true
}

tasks.assemble {
  dependsOn(tasks.shadowJar)
}

java {
  withSourcesJar()
  withJavadocJar()
}

tasks {
  withType<Test> {
    useJUnitPlatform()
  }
  withType<ShadowJar> {
    // <WORKAROUND for="https://github.com/johnrengelman/shadow/issues/448">
    configurations = listOf(
      project.configurations.implementation.get(),
      project.configurations.runtimeOnly.get()
    ).onEach { it.isCanBeResolved = true }
  }
}
