package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class SearchResponseMatchers {

    public static Matcher<SearchResponse> hasHits(final Matcher<SearchHits> matcher) {
        return new TypeSafeMatcher<SearchResponse>() {
            private SearchHits got;
            @Override public boolean matchesSafely(SearchResponse searchResponse) {
                got = searchResponse.hits();
                return matcher.matches(got);
            }

            @Override public void describeTo(Description description) {
                description.appendText(" hits ")
                        .appendDescriptionOf(matcher)
                        .appendText(" but got")
                        .appendValue(got);
            }
        };
    }

    public static Matcher<SearchHits> hasTotalHits(final Matcher<Long> matcher) {
        return new TypeSafeMatcher<SearchHits>() {
            private long totalHits;
            @Override public boolean matchesSafely(SearchHits hits) {
                totalHits = hits.getTotalHits();
                return matcher.matches(totalHits);
            }

            @Override public void describeTo(Description description) {
                description.appendText(" total hits ")
                        .appendDescriptionOf(matcher)
                        .appendText(" but got")
                        .appendValue(totalHits);
            }
        };
    }

    public static Matcher<SearchHits> hasHitAtPosition(final int position, final Matcher<SearchHit> matcher) {
        return new TypeSafeMatcher<SearchHits>() {
            @Override public boolean matchesSafely(SearchHits searchHits) {
                return matcher.matches(searchHits.getAt(position));
            }

            @Override public void describeTo(Description description) {
                description.appendText(" expected at position ")
                        .appendValue(position)
                        .appendDescriptionOf(matcher);
            }
        };
    }

    public static Matcher<SearchHit> hasId(final Matcher<String> matcher) {
        return new TypeSafeMatcher<SearchHit>() {
            String got;
            @Override public boolean matchesSafely(SearchHit id) {
                got = id.getId();
                return matcher.matches(got);
            }

            @Override public void describeTo(Description description) {
                description.appendText(" expected id ").appendDescriptionOf(matcher)
                        .appendText(", but got " )
                        .appendValue(got);
            }
        };
    }
}
