package org.neo4j.graphalgo;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/**
 * A matcher that applies a delegate matcher to the root cause of the current Throwable, returning the result of that
 * match.
 *
 * @param <T> the type of the throwable being matched
 */
public class ThrowableRootCauseMatcher<T extends Throwable> extends TypeSafeMatcher<T> {

    private final Matcher<? extends Throwable> causeMatcher;

    public ThrowableRootCauseMatcher(Matcher<? extends Throwable> causeMatcher) {
        this.causeMatcher = causeMatcher;
    }

    public void describeTo(Description description) {
        description.appendText("exception with root cause ");
        description.appendDescriptionOf(causeMatcher);
    }

    @Override
    protected boolean matchesSafely(T item) {
        Throwable rootCause = ExceptionUtil.rootCause(item);
        return causeMatcher.matches(rootCause);
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("cause ");
        Throwable rootCause = ExceptionUtil.rootCause(item);
        causeMatcher.describeMismatch(rootCause, description);
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause for which the supplied matcher
     * evaluates to true.
     *
     * @param matcher to apply to the root cause of the outer exception
     * @param <T> type of the outer exception
     */
    @Factory
    public static <T extends Throwable> Matcher<T> rootCause(final Matcher<? extends Throwable> matcher) {
        return new ThrowableRootCauseMatcher<>(matcher);
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause of the given type.
     *
     * @param type the type/class to match the root cause against
     * @param <T> type of the outer exception
     */
    @Factory
    public static <T extends Throwable> Matcher<T> rootCause(final Class<? extends Throwable> type) {
        return rootCause(instanceOf(type));
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause with the given message.
     *
     * @param message the expected message of the root cause, verified with an equals comparison
     * @param <T> type of the outer exception
     */
    @Factory
    public static <T extends Throwable> Matcher<T> rootCauseMessage(final String message) {
        return rootCause(hasMessage(equalTo(message)));
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause with the given message.
     *
     * @param message the expected message of the root cause, verified with an contains comparison
     * @param <T> type of the outer exception
     */
    @Factory
    public static <T extends Throwable> Matcher<T> rootCauseMessageSubString(final String message) {
        return rootCause(hasMessage(containsString(message)));
    }
}
