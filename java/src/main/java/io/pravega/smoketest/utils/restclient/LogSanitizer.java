package io.pravega.smoketest.utils.restclient;

import java.net.URI;

/**
 * This interface provides a way to sanitize sensitive logs going to the logging filter.
 */
public interface LogSanitizer {
    /**
     * Sanitizes an entity before logging, allows for stripping out any sensitive information.
     * 
     * @param requestUri
     *            the request URI.
     * @param entity
     *            the entity as a string.
     * @param truncated
     *            whether the entity was truncated.
     * @return the sanitized entity.
     */
    public String sanitizeEntity(URI requestUri, String entity, boolean truncated);
}
