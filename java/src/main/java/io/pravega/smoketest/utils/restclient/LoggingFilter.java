package io.pravega.smoketest.utils.restclient;

import org.slf4j.Logger;

import javax.annotation.Priority;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@Priority(Integer.MIN_VALUE)
public class LoggingFilter implements ClientRequestFilter, ClientResponseFilter, WriterInterceptor {

    private static final String REQUEST_ID_PROPERTY = LoggingFilter.class.getName() + ".requestId";
    private static final String START_TIME_PROPERTY = LoggingFilter.class.getName() + ".startTime";
    private static final String ENTITY_STREAM_PROPERTY = LoggingFilter.class.getName() + ".entityStream";
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static AtomicLong id = new AtomicLong(0);
    private final int maxEntityLength;
    private final Logger logger;
    private final LogSanitizer sanitizer;

    public LoggingFilter(Logger logger) {
        this(logger, 1024);
    }

    public LoggingFilter(Logger logger, LogSanitizer sanitizer) {
        this(logger, 1024, sanitizer);
    }

    public LoggingFilter(Logger logger, int maxEntityLength) {
        this(logger, maxEntityLength, null);
    }

    public LoggingFilter(Logger logger, int maxEntityLength, LogSanitizer sanitizer) {
        this.logger = logger;
        this.maxEntityLength = maxEntityLength;
        this.sanitizer = sanitizer;
    }

    @Override
    public void filter(ClientRequestContext requestContext) throws IOException {
        if (!logger.isInfoEnabled()) {
            return;
        }

        long requestId = id.getAndIncrement();
        requestContext.setProperty(REQUEST_ID_PROPERTY, requestId);
        long startTime = System.currentTimeMillis();
        requestContext.setProperty(START_TIME_PROPERTY, startTime);

        URI requestUri = requestContext.getUri();
        StringBuilder b = new StringBuilder();
        b.append(requestId).append(" > ").append(requestContext.getMethod());
        b.append(" ").append(requestUri.toASCIIString());

        // Only log entities in DEBUG
        if (logger.isDebugEnabled() && maxEntityLength > 0 && requestContext.hasEntity()) {
            final OutputStream stream = new LoggingStream(requestUri, b, requestContext.getEntityStream());
            requestContext.setEntityStream(stream);
            requestContext.setProperty(ENTITY_STREAM_PROPERTY, stream);
        }
        else {
            log(b);
        }
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) throws IOException {
        if (!logger.isInfoEnabled()) {
            return;
        }

        long requestId = (Long) requestContext.getProperty(REQUEST_ID_PROPERTY);
        long startTime = (Long) requestContext.getProperty(START_TIME_PROPERTY);
        long endTime = System.currentTimeMillis();

        StringBuilder b = new StringBuilder();
        b.append(requestId).append(" < ").append(responseContext.getStatus());
        b.append("  took ").append(endTime - startTime).append(" ms");

        // Only log entities in DEBUG
        if (logger.isDebugEnabled() && maxEntityLength > 0 && responseContext.hasEntity()) {
            URI requestUri = requestContext.getUri();
            responseContext.setEntityStream(logInboundEntity(requestUri, b, responseContext.getEntityStream()));
        }

        log(b);
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        final LoggingStream stream = (LoggingStream) context.getProperty(ENTITY_STREAM_PROPERTY);
        context.proceed();
        if (stream != null) {
            log(stream.getStringBuilder());
        }
    }

    private String sanitize(URI requestUri, String entity, boolean truncated) {
        if (sanitizer != null) {
            return sanitizer.sanitizeEntity(requestUri, entity, truncated);
        }
        return entity;
    }

    private void log(StringBuilder sb) {
        logger.info(sb.toString());
    }

    private InputStream logInboundEntity(final URI requestUri, final StringBuilder b, InputStream stream)
            throws IOException {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(maxEntityLength + 1);
        final byte[] entity = new byte[maxEntityLength + 1];
        final int entitySize = stream.read(entity);

        String entityStr = new String(entity, 0, Math.min(entitySize, maxEntityLength), DEFAULT_CHARSET);
        boolean truncated = entitySize > maxEntityLength;

        b.append("\n");
        b.append(sanitize(requestUri, entityStr, truncated));
        if (truncated) {
            b.append("...");
        }
        stream.reset();
        return stream;
    }

    private class LoggingStream extends FilterOutputStream {
        private final URI requestUri;
        private final StringBuilder sb;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        LoggingStream(URI requestUri, StringBuilder sb, OutputStream out) {
            super(out);
            this.requestUri = requestUri;
            this.sb = sb;
        }

        StringBuilder getStringBuilder() {
            // write entity to the builder
            final byte[] entity = buffer.toByteArray();
            boolean truncated = entity.length > maxEntityLength;
            String entityStr = new String(entity, 0, entity.length, DEFAULT_CHARSET);

            sb.append("\n");
            sb.append(sanitize(requestUri, entityStr, truncated));
            if (truncated) {
                sb.append("...");
            }

            return sb;
        }

        @Override
        public void write(final int i) throws IOException {
            if (buffer.size() <= maxEntityLength) {
                buffer.write(i);
            }
            out.write(i);
        }
    }
}