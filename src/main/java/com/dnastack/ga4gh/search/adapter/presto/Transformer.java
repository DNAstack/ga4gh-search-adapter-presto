package com.dnastack.ga4gh.search.adapter.presto;

@FunctionalInterface
public interface Transformer {

    /**
     *
     * @param content The data to transform
     * @return A transformed version of content.  Content may need to be transformed if it is in a format incompatible
     * with the search specification.
     */
    String transform(String content);
}
