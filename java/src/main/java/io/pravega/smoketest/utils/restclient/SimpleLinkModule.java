package io.pravega.smoketest.utils.restclient;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;

import javax.ws.rs.core.Link;
import java.io.IOException;

public class SimpleLinkModule extends SimpleModule {
    private static final long serialVersionUID = -4997139608862599137L;

    public SimpleLinkModule() {
        this.addDeserializer(Link.class, new LinkDeserializer());
        this.addSerializer(Link.class, new LinkSerializer());
    }

    @Override
    public String getModuleName() {
        return SimpleLinkModule.class.getName();
    }

    @Override
    public Version version() {
        return Version.unknownVersion();
    }

    class LinkDeserializer extends JsonDeserializer<Link> {
        @Override
        public Link deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String href = node.get("href").textValue();
            String rel = node.get("rel").textValue();
            return Link.fromUri(href).rel(rel).build();
        }
    }

    class LinkSerializer extends JsonSerializer<Link> {
        @Override
        public void serialize(Link value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeStringField("href", value.getUri().toString());
            jgen.writeStringField("rel", value.getRel());
            jgen.writeEndObject();
        }
    }

}
