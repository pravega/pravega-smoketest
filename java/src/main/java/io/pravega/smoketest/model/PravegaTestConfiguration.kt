package io.pravega.smoketest.model

import io.pravega.smoketest.model.payload.PravegaTaskParametersPayload
import io.pravega.smoketest.model.payload.TaskParametersPayload
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
class PravegaTestConfiguration @JsonCreator
constructor() : TestConfiguration<PravegaTaskConfiguration>(TestType.Pravega) {
    var tasks = mutableListOf<PravegaTaskConfiguration>()

    // Create stream if it doesn't exist
    var createStream = true

    // Delete stream after test
    var deleteStream = true


    // Task level overrides
    var payload: PayloadConfiguration? = null
    var scope: String? = null
    var stream: String? = null
    var transactional: Boolean? = false
    var transactionSize: Int? = null
    var throttle: ThrottleConfiguration? = null
    var streamPolicies: StreamPolicies? = null

    @JsonIgnore
    fun getTasksByStream(): Map<String, List<PravegaTaskConfiguration>> {
        return tasks.groupBy { it.getStreamFQN() }
    }

    @JsonIgnore
    fun getTasksByReaderGroup(): Map<String, List<PravegaTaskConfiguration>> {
        return tasks.filter { it.readerGroup != null }
            .groupBy { it.readerGroup!! }
    }

    override fun applyGlobalOptions() {
        super.applyGlobalOptions()

        val finalTasks =  mutableListOf<PravegaTaskConfiguration>()

        tasks.forEach {task->
            task.minutes = minutes
            task.forever = forever
            if (task.forever) {
                task.minutes = -1
            }
            transactional?.let { task.transactional = it}
            transactionSize?.let { task.transactionSize = it}
            scope?.let { task.scope = it}
            stream?.let { task.stream = it}
            streamPolicies?.let { task.streamPolicies = it}
            throttle?.let { task.throttle = it}
            payload?.let { task.payload = it}
            for (copy in 1..task.duplicates) {
                finalTasks.add(task)
            }
            // everything has been duplicated now
            task.duplicates = 1
        }

        this.tasks = finalTasks

    }

    override fun getLaunchableTasks(): List<TaskParametersPayload<PravegaTaskConfiguration>> {
        return tasks.map { PravegaTaskParametersPayload(it) }.toList()
    }
}
