package io.pravega.smoketest.utils.restclient;

import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.Base64;

public class RestClientUtil {
    public static final int DEFAULT_CONNECT_TIMEOUT = 30000;
    public static final int DEFAULT_READ_TIMEOUT = 30000;
    public static final int DEFAULT_MAX_ENTITY_SIZE = 1024;

    /**
     * Creates the default client used for all rest client.
     *
     * @param logger the logger for logging the requests.
     * @return the client.
     */
    public static Client defaultClient(Logger logger) {
        return defaultClient(logger, null);
    }

    /**
     * Creates the default client used for all rest client.
     *
     * @param logger    the logger for logging the requests.
     * @param sanitizer the sanitizer for preventing sensitive data being logged.
     * @return the client.
     */
    public static Client defaultClient(Logger logger, LogSanitizer sanitizer) {
        ClientBuilder builder = ClientBuilder.newBuilder();
        builder.register(JacksonObjectMapperProvider.class);
        builder.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
        builder.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        builder.sslContext(SSLUtil.getTrustAllContext());
        builder.hostnameVerifier(SSLUtil.getNullHostnameVerifier());
        builder.register(new LoggingFilter(logger, DEFAULT_MAX_ENTITY_SIZE, sanitizer));
        return builder.build();
    }

    /**
     * Creates a basic auth token for the given username/password, which is a base 64 encoded version of
     * <tt>&lt;username&gt;:&lt;password&gt;</tt>
     *
     * @param username the username
     * @param password the password
     * @return the token used for basic auth.
     */
    public static String basicAuthToken(String username, String password) {
        return Base64.getEncoder().encodeToString(new String(username + ":" + password).getBytes());
    }
}
