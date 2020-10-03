package com.dnastack.ga4gh.search.client.tablesregistry;

import com.dnastack.ga4gh.search.client.tablesregistry.model.AccessToken;
import com.dnastack.ga4gh.search.client.tablesregistry.model.OAuthRequest;
import feign.Headers;
import feign.RequestLine;
import org.springframework.http.MediaType;

public interface OAuthClient {
    @RequestLine("POST")
    @Headers("Content-Type: " + MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    AccessToken getToken(OAuthRequest request);
}
