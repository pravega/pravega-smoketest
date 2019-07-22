package io.pravega.smoketest.model

import io.pravega.smoketest.utils.PravegaNamingUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import java.util.concurrent.TimeUnit

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class PravegaTaskConfiguration {
    // Properties of the task only
    var numReaders: Int = 0
    var numForgetfulReaders: Int = 0
    var attentionSpan: Long = TimeUnit.HOURS.toMinutes(12)
    var numWriters: Int = 0
    var readerType: ReaderType = ReaderType.TAIL

    // Properties that can be set on the task or inherited from the parent
    var transactional: Boolean = false
    var transactionSize: Int = 10
    var scope: String? = null
    var stream: String? = null
    var streamPolicies: StreamPolicies? = null
    var payload = PayloadConfiguration()
    var readerGroup: String? = null
        get() = field ?: PravegaNamingUtils.defaultReaderGroupName(scope, stream)
    var throttle: ThrottleConfiguration? = null

    // Properties always set by the parent test
    var forever = false
    var minutes: Long = 1
    var duplicates = 1

    @JsonIgnore
    fun getStreamFQN(): String = "${scope}/${stream}"
}
