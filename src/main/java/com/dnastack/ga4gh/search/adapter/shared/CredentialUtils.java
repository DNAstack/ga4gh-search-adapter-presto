package com.dnastack.ga4gh.search.adapter.shared;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CredentialUtils {
    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
            .map(val -> val.split("=", 2))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }
}
