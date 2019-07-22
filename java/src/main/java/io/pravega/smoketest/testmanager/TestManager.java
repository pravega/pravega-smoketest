package io.pravega.smoketest.testmanager;

import io.pravega.smoketest.testmanager.resources.ManagerResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManager extends Application<ManagerConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(TestManager.class);

    public static void main(String[] args) {
        try {
            new TestManager().run(args);
        }
        catch (Exception e) {
            System.err.println("Error starting Test Manager");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    @Override
    public void initialize(Bootstrap<ManagerConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false)));
    }

    @Override
    public void run(ManagerConfiguration configuration, Environment environment) throws Exception {
        Injector injector = createInjector(configuration);

        environment.jersey().register(new JsonProcessingExceptionMapper(true));
        environment.jersey().register(injector.getInstance(ManagerResource.class));

        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(injector.getInstance(ObjectMapper.class));
        environment.jersey().register(provider);
    }

    private Injector createInjector(ManagerConfiguration configuration) {
        return Guice.createInjector(new ManagerModule(configuration));
    }
}