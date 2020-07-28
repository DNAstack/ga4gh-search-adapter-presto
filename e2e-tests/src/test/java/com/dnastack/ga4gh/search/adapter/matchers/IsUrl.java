package com.dnastack.ga4gh.search.adapter.matchers;

import org.apache.commons.validator.routines.UrlValidator;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class IsUrl extends TypeSafeMatcher<String> {

    @Override
    public boolean matchesSafely(String url) {
        return UrlValidator.getInstance().isValid(url);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("not a URL");
    }

    public static Matcher isUrl() {
        return new IsUrl();
    }

}
