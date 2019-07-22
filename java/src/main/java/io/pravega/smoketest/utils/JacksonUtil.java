package io.pravega.smoketest.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Jackson-related utilities.
 */
public class JacksonUtil {

    /**
     * Creates an {@klink ObjectMapper} suitable for use by the agent.
     */
    public static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // find and register extra modules (Java8 support)
        mapper.findAndRegisterModules();

        return mapper;
    }

    private JacksonUtil() {}
}
