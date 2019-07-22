package io.pravega.smoketest.testmanager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;

import java.net.URI;

public class ManagerModule extends AbstractModule {
    private ManagerConfiguration config;

    public ManagerModule(ManagerConfiguration config) {
        this.config = config;
    }

    protected void configure() {
        bind(ObjectMapper.class).toInstance(this.getObjectMapper());

        bindValue(URI.class, "controllerUri", URI.create(config.getPravegaControllerUri()));
        bindString("configPath", config.getConfigPath());
    }

    private void bindString(String name, String value) {
        bindValue(String.class, name, value);
    }

    private void bindBoolean(String name, Boolean value) {
        bindValue(Boolean.class, name, value);
    }

    private <T> void bindValue(Class<T> type, String name, T value) {
        if (value != null) {
            bind(type).annotatedWith(Names.named(name)).toInstance(value);
        }
        else {
            bind(type).annotatedWith(Names.named(name)).toProvider(Providers.of(null));
        }
    }

    private ObjectMapper getObjectMapper() {
        return new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(true))
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(new KotlinModule())
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }
}
