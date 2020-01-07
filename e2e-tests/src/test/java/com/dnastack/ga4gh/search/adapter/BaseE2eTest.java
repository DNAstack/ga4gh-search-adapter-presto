package com.dnastack.ga4gh.search.adapter;

import static org.junit.Assert.fail;

import io.restassured.RestAssured;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseE2eTest {

    @BeforeClass
    public static void setUp() throws Exception {
        RestAssured.baseURI = requiredEnv("E2E_BASE_URI");
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
}
