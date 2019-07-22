package io.pravega.smoketest.testmanager

import io.dropwizard.Configuration

data class ManagerConfiguration (
    var pravegaControllerUri :String = "",
    var configPath: String = ""
): Configuration()