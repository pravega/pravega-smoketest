package io.pravega.smoketest.model.payload

import io.pravega.smoketest.model.PravegaTaskConfiguration
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class PravegaTaskParametersPayload : TaskParametersPayload<PravegaTaskConfiguration> {

    @JsonCreator
    constructor(@JsonProperty("taskConfiguration") taskConfiguration : PravegaTaskConfiguration) : super(taskConfiguration)
}
