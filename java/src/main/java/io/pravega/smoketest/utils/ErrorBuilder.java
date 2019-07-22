package io.pravega.smoketest.utils;

import javax.ws.rs.core.Response;

public abstract class ErrorBuilder {
    private static Response error(int code, String message) {
        String blob = "{\"error\": {\"status\": " + code + ", \"message\": \"" + message + "\"}}";
        return Response.status(code).entity(blob).build();
    }

    public static Response serviceUnavailable() {
        return error(503, "Service Unavailable");
    }
}
