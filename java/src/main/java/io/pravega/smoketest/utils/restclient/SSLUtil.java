package io.pravega.smoketest.utils.restclient;

import javax.net.ssl.*;
import java.security.SecureRandom;

/**
 * SSL Utilities such as trusting all SSL Certificates.
 */
public class SSLUtil {
    private static SSLContext trustAllContext;
    private static SSLSocketFactory trustAllSslSocketFactory;
    private static NullHostNameVerifier nullHostnameVerifier;

    public static NullHostNameVerifier getNullHostnameVerifier() {
        if (nullHostnameVerifier == null) {
            nullHostnameVerifier = new NullHostNameVerifier();
        }
        return nullHostnameVerifier;
    }

    public static SSLSocketFactory getTrustAllSslSocketFactory() {
        if (trustAllSslSocketFactory == null) {
            SSLContext sc = getTrustAllContext();
            trustAllSslSocketFactory = sc.getSocketFactory();
        }
        return trustAllSslSocketFactory;
    }

    public static SSLContext getTrustAllContext() {
        if (trustAllContext == null) {
            try {
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, newTrustManagers(), new SecureRandom());
                trustAllContext = sc;
            }
            catch (Exception e) {
                throw new RuntimeException("Unable to register SSL TrustManager to trust all SSL Certificates", e);
            }
        }
        return trustAllContext;
    }

    private static TrustManager[] newTrustManagers() {
        return new TrustManager[]{new AllTrustManager()};
    }

    private static class AllTrustManager implements X509TrustManager {
        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override
        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        }

        @Override
        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
        }
    }

    private static class NullHostNameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(String arg0, SSLSession arg1) {
            return true;
        }
    }
}
