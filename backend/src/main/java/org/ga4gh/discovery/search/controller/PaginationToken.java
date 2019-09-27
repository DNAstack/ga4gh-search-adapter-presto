package org.ga4gh.discovery.search.controller;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class PaginationToken {

    private final int pageIndex;
    private final int pageSize;

    public int getFromIndex() {
        return pageSize * pageIndex;
    }

    public int getToIndex() {
        return getFromIndex() + pageSize;
    }

    public static PaginationToken parse(String encodedToken) {
        try {
            if (encodedToken == null) {
                return null;
            }
            String decodedToken = URLDecoder.decode(encodedToken, "UTF-8");
            String[] parts = decodedToken.split(":");
            checkArgument(parts.length == 2, "Not a pagination token: " + decodedToken);
            int pageIndex = Integer.parseInt(parts[0]);
            int pageSize = Integer.parseInt(parts[1]);
            return of(pageIndex, pageSize);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unable to decode: " + encodedToken, e);
        }
    }

    public static PaginationToken of(int pageIndex, int pageSize) {
        checkArgument(pageIndex >= 0, "Page index must not be negative");
        checkArgument(pageSize >= 0, "Page size must be greater than 1");
        return new PaginationToken(pageIndex, pageSize);
    }

    public String encode() {
        StringBuffer s = new StringBuffer();
        s.append(Long.toString(pageIndex));
        s.append(":");
        s.append(Long.toString(pageSize));
        try {
            return URLEncoder.encode(s.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new UncheckedIOException("Unable to encode: " + s.toString(), e);
        }
    }

    public boolean hasPrevious() {
        return pageIndex > 0;
    }

    public Optional<PaginationToken> previous() {
        return hasPrevious() ? Optional.of(of(pageIndex - 1, pageSize)) : Optional.empty();
    }

    public Optional<PaginationToken> next(int total) {
        return getToIndex() >= total ? Optional.empty() : Optional.of(of(pageIndex + 1, pageSize));
    }
}
