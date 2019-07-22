package io.pravega.smoketest.utils.restclient;

import org.glassfish.jersey.client.HttpUrlConnectorProvider;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple wrapper around a JAX-RS client to make operations simpler
 */
public class RestClient {
    private final WebTarget target;
    private MediaType contentType;
    private MediaType acceptType;
    private MultivaluedMap<String, Object> headers;
    private Map<String, Object> jerseyClientProperties;

    public RestClient(WebTarget target) {
        this(target, MediaType.APPLICATION_JSON_TYPE);
    }

    public RestClient(WebTarget target, MediaType mediaType) {
        this(target, mediaType, mediaType);
    }

    public RestClient(WebTarget target, MediaType contentType, MediaType acceptType) {
        this(target, contentType, acceptType, new MultivaluedHashMap<>(), new HashMap<>());
    }

    public RestClient(WebTarget target, MediaType contentType, MediaType acceptType, MultivaluedMap<String, Object> headers, Map<String, Object> jerseyClientProperties) {
        this.target = target;
        this.contentType = contentType;
        this.acceptType = acceptType;
        this.headers = headers;
        this.jerseyClientProperties = jerseyClientProperties;
    }

    public WebTarget getTarget() {
        return target;
    }

    public MediaType getContentType() {
        return contentType;
    }

    public MediaType getAcceptType() {
        return acceptType;
    }

    public Response get(String path) {
        return builder(path).get();
    }

    public Response get(String path, Map<String, Object> queryParams) {
        return builder(path, queryParams).get();
    }

    public <T> T get(String path, Class<T> responseType) {
        return builder(path).get(responseType);
    }

    public <T> T get(String path, GenericType<T> responseType) {
        return builder(path).get(responseType);
    }

    public <T> T get(String path, Map<String, Object> queryParams, Class<T> responseType) {
        return builder(path, queryParams).get(responseType);
    }

    public <T> T get(String path, Map<String, Object> queryParams, GenericType<T> responseType) {
        return builder(path, queryParams).get(responseType);
    }

    public <P> Response put(String path, P payload) {
        return builder(path).put(Entity.entity(payload, contentType));
    }

    public <T, P> T put(String path, P payload, Class<T> responseType) {
        return builder(path).put(Entity.entity(payload, contentType), responseType);
    }

    public <T, P> T put(String path, P payload, GenericType<T> responseType) {
        return builder(path).put(Entity.entity(payload, contentType), responseType);
    }

    public <P> Response put(String path, Map<String, Object> queryParams, P payload) {
        return builder(path, queryParams).put(Entity.entity(payload, contentType));
    }

    public <T, P> T put(String path, Map<String, Object> queryParams, P payload, Class<T> responseType) {
        return builder(path, queryParams).put(Entity.entity(payload, contentType), responseType);
    }

    public <T, P> T put(String path, Map<String, Object> queryParams, P payload, GenericType<T> responseType) {
        return builder(path, queryParams).put(Entity.entity(payload, contentType), responseType);
    }

    public <P> Response patch(String path, P payload) {
        return builder(path)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .method("PATCH", Entity.entity(payload, contentType));
    }

    public <P> Response post(String path, P payload) {
        return builder(path).post(Entity.entity(payload, contentType));
    }

    public <P> Response post(String path, Map<String, Object> queryParams, P payload) {
        return builder(path, queryParams).post(Entity.entity(payload, contentType));
    }

    public <T, P> T post(String path, P payload, Class<T> responseType) {
        return builder(path).post(Entity.entity(payload, contentType), responseType);
    }

    public <T, P> T post(String path, Map<String, Object> queryParams, P payload, Class<T> responseType) {
        return builder(path, queryParams).post(Entity.entity(payload, contentType), responseType);
    }

    public <T, P> T post(String path, P payload, GenericType<T> responseType) {
        return builder(path).post(Entity.entity(payload, contentType), responseType);
    }

    public <T, P> T post(String path, Map<String, Object> queryParams, P payload, GenericType<T> responseType) {
        return builder(path, queryParams).post(Entity.entity(payload, contentType), responseType);
    }

    public Response delete(String path) {
        return builder(path).delete();
    }

    public Response delete(String path, Map<String, Object> queryParams) {
        return builder(path, queryParams).delete();
    }

    public <T> T delete(String path, Class<T> responseType) {
        return builder(path).delete(responseType);
    }

    public <T> T delete(String path, GenericType<T> responseType) {
        return builder(path).delete(responseType);
    }

    public <T> T delete(String path, Map<String, Object> queryParams, Class<T> responseType) {
        return builder(path, queryParams).delete(responseType);
    }

    public <T> T delete(String path, Map<String, Object> queryParams, GenericType<T> responseType) {
        return builder(path, queryParams).delete(responseType);
    }

    public RestClient clone() {
        return new RestClient(target, contentType, acceptType, new MultivaluedHashMap<String, Object>(headers), new HashMap<>(jerseyClientProperties));
    }

    /**
     * Set the content type of the request.
     *
     * @param contentType The MediaType of the entity that will be sent.
     * @return this
     */
    public RestClient contentType(MediaType contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Acts as a passthrough for properties to Jersey {@link WebTarget#property(String, Object)}.
     * Reference for possibile properties are in {@link org.glassfish.jersey.client.HttpUrlConnectorProvider}
     * and {@link org.glassfish.jersey.client.ClientProperties}
     * @param property Name of property
     * @param value Value to set
     * @return this
     */
    public RestClient property(String property, Object value) {
        this.jerseyClientProperties.put(property, value);
        return this;
    }

    /**
     * Set the content type of the request
     *
     * @param entityType The Content-Type header string.
     * @return this
     */
    public RestClient contentType(String entityType) {
        return contentType(MediaType.valueOf(entityType));
    }

    /**
     * Set the accept type of the response
     *
     * @param acceptType The MediaType of the response.
     * @return this
     */
    public RestClient accept(MediaType acceptType) {
        this.acceptType = acceptType;
        return this;
    }

    /**
     * Set the accept type of the response.
     *
     * @param acceptType The Accept header string.
     * @return this
     */
    public RestClient accept(String acceptType) {
        return accept(MediaType.valueOf(acceptType));
    }

    /**
     * Add a single header.
     *
     * @param key   the key
     * @param value the single value of the key
     * @return this
     */
    public RestClient header(String key, Object value) {
        headers.putSingle(key, value);
        return this;
    }

    /**
     * Adds basic authentication.
     *
     * @param username the username
     * @param password the password
     * @return this
     */
//    public RestClient basicAuth(String username, String password) {
//        return header("Authorization", "Basic " + RestClientUtil.basicAuthToken(username, password));
//    }

    /**
     * Adds a bearer token.
     *
     * @param token the token.
     * @return this
     */
    public RestClient bearerToken(String token) {
        return header("Authorization", "Bearer " + token);
    }

    protected Builder builder(String path) {
        return builder(path, null);
    }

    protected Builder builder(String path, Map<String, Object> queryParams) {
        WebTarget newTarget = target.path(path);
        if (queryParams != null) {
            for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                newTarget = newTarget.queryParam(key, value);
            }
        }
        Builder preppedBuilder = newTarget.request().headers(headers).accept(acceptType);
        for (String property : this.jerseyClientProperties.keySet()) {
            preppedBuilder.property(property, this.jerseyClientProperties.get(property));
        }
        return preppedBuilder;
    }
}
