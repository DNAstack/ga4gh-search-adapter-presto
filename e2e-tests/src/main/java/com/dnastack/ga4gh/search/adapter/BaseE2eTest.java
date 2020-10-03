package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.matchers.IsUrl;
import com.dnastack.ga4gh.search.adapter.test.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.http.Method;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Supplier;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.request;
import static io.restassured.http.Method.GET;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
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
