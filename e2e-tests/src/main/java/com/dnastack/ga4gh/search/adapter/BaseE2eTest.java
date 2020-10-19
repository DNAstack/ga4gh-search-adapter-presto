package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.matchers.IsUrl;
import io.restassured.RestAssured;
import io.restassured.config.RestAssuredConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.fail;

@Slf4j
public class BaseE2eTest {

    static Boolean useSSL;
    static RestAssuredConfig config;

    @BeforeClass
    public static void setupRestAssured() {
        RestAssured.baseURI = requiredEnv("E2E_BASE_URI");
        try {
            if (new URI(RestAssured.baseURI).getHost().equalsIgnoreCase("localhost")) {
                log.info("E2E BASE URI is at localhost, allowing localhost to occur within URLs of JSON responses.");
                IsUrl.setAllowLocalhost(true);
                useSSL=false;
            } else {
                useSSL=true;
            }
        } catch (URISyntaxException use) {
            throw new RuntimeException(String.format("Error initializing tests -- E2E_BASE_URI (%s) is invalid", RestAssured.baseURI));
        }
    }

    protected static String requiredEnv(String name) {
        String val = System.getenv(name);
        if (val == null) {
            fail("Environnment variable `" + name + "` is required");
        }
        return val;
    }

    protected static String optionalEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val == null) {
            return defaultValue;
        }
        return val;
    }

    interface ExceptionalSupplier<T, E extends Exception> {
        T get() throws E;
    }

    protected static <E extends Exception> String lazyOptionalEnv(String name, ExceptionalSupplier<String, E> defaultValue) throws E {
        String val = System.getenv(name);
        if (val == null) {
            return defaultValue.get();
        }
        return val;
    }

}
