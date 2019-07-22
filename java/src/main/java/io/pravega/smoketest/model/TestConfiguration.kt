package io.pravega.smoketest.model

import io.pravega.smoketest.testmanager.manager.AssertionsRunner
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

import java.util.UUID

import io.pravega.smoketest.model.payload.TaskParametersPayload
import com.fasterxml.jackson.annotation.JsonIgnore

@JsonTypeInfo(
    use = Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes(
    Type(value = PravegaTestConfiguration::class, name = "Pravega"))
abstract class TestConfiguration<T: Any>(val type: TestType) {
    var forever = false
    var minutes: Long = 1

    var id = UUID.randomUUID().toString()
    var name = "${type} Test - ${id}"
    var smoketestAssertions = HashMap<String, Long>()

    fun validateConfiguration() {
        if (!AssertionsRunner.ASSERTION_KEYS.containsAll(smoketestAssertions.keys)) {
            val validKeys = AssertionsRunner.ASSERTION_KEYS.toString()
            throw IllegalStateException("Invalid smoketest assertion key. Expected: " + validKeys)
        }
    }

    open fun applyGlobalOptions() {
        if (this.forever) {
            this.minutes = -1
        }
    }

    @JsonIgnore
    abstract fun getLaunchableTasks(): List<TaskParametersPayload<T>>
}
