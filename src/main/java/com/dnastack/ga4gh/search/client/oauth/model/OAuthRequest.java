package com.dnastack.ga4gh.search.client.oauth.model;

import feign.form.FormProperty;
import lombok.Data;

@Data
public class OAuthRequest {
    @FormProperty("grant_type")
    String grantType;

    @FormProperty("client_id")
    String clientId;

    @FormProperty("client_secret")
    String clientSecret;

    @FormProperty("audience")
    String audience;
}
