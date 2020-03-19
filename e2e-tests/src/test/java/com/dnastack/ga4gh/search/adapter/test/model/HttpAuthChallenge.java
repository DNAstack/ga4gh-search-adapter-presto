package com.dnastack.ga4gh.search.adapter.test.model;

import lombok.Value;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Value
public class HttpAuthChallenge {
    String scheme;
    Map<String, String> params;

    private enum ParseState {READ_SCHEME, READ_KEY, DECIDE_VALUE_TYPE, READ_QUOTED_VALUE, READ_UNQUOTED_VALUE}

    public static HttpAuthChallenge fromString(String headerValue) {
        StringBuilder sb = new StringBuilder();
        ParseState state = ParseState.READ_SCHEME;
        String scheme = null;
        String currentKey = null;
        Map<String, String> params = new HashMap<>();

        for (int i = 0; i < headerValue.length(); i++) {
            char ch = headerValue.charAt(i);
            switch(state) {
                case READ_SCHEME:
                    if (ch == ' ') {
                        scheme = sb.toString();
                        sb = new StringBuilder();
                        state = ParseState.READ_KEY;
                    } else {
                        sb.append(ch);
                    }
                    break;
                case READ_KEY:
                    if (ch == '=') {
                        currentKey = sb.toString();
                        sb = new StringBuilder();
                        state = ParseState.DECIDE_VALUE_TYPE;
                    } else {
                        sb.append(ch);
                    }
                    break;
                case DECIDE_VALUE_TYPE:
                    if (ch == ' ') {
                        // skip spaces
                    } else if (ch == ',') {
                        // skip commas (value separators)
                    } else if (ch == '"') {
                        state = ParseState.READ_QUOTED_VALUE;
                    } else {
                        state = ParseState.READ_UNQUOTED_VALUE;
                    }
                    break;
                case READ_UNQUOTED_VALUE:
                    if (ch == ' ' || ch == ',') {
                        params.put(requireNonNull(currentKey), sb.toString());
                        currentKey = null;
                        sb = new StringBuilder();
                        state = ParseState.DECIDE_VALUE_TYPE;
                    } else {
                        sb.append(ch);
                    }
                    break;
                case READ_QUOTED_VALUE:
                    if (ch == '"') {
                        params.put(requireNonNull(currentKey), sb.toString());
                        currentKey = null;
                        sb = new StringBuilder();
                        state = ParseState.DECIDE_VALUE_TYPE;
                    } else if (ch == '\\') {
                        i++;
                        ch = headerValue.charAt(i);
                        sb.append(ch);
                    } else {
                        sb.append(ch);
                    }
                    break;
            }
        }

        // end of string reached - clean up if it's a valid end state
        switch (state) {
            case READ_SCHEME:
                scheme = sb.toString();
                break;
            case READ_KEY:
                throw new IllegalArgumentException("key without value in " + headerValue);
            case DECIDE_VALUE_TYPE:
                // this is good. we are between key/value pairs.
                break;
            case READ_QUOTED_VALUE:
                throw new IllegalArgumentException("unterminated quoted value in " + headerValue);
            case READ_UNQUOTED_VALUE:
                params.put(requireNonNull(currentKey), sb.toString());
                break;
        }

        return new HttpAuthChallenge(scheme, params);
    }
}
