package io.pravega.smoketest.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class PravegaNamingUtils {
    public static String defaultReaderGroupName(String scope, String stream) {
        return StringUtils.capitalize(scope) + StringUtils.capitalize(stream);
    }

    public static String newReaderId(String scope, String stream)  {
        return newStreamBasedId(scope, stream);
    }

    private static String newStreamBasedId(String scope, String stream) {
        return newId(StringUtils.capitalize(scope) + StringUtils.capitalize(stream));
    }

    private static String newId(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }
}
