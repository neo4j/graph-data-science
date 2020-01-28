/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.graphalgo.compat.ExceptionUtil;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.neo4j.graphalgo.core.ExceptionMessageMatcher.exceptionMessage;

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
    public static <T extends Throwable> Matcher<T> rootCause(final Matcher<? extends Throwable> matcher) {
        return new ThrowableRootCauseMatcher<>(matcher);
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause of the given type.
     *
     * @param type the type/class to match the root cause against
     * @param <T> type of the outer exception
     */
    public static <T extends Throwable> Matcher<T> rootCause(final Class<? extends Throwable> type) {
        return rootCause(instanceOf(type));
    }

    /**
     * Returns a matcher that verifies that the outer exception has a root cause of the given type and
     * with the given message.
     *
     * @param type the type/class to match the root cause against
     * @param message the expected message of the root cause, verified with an equals comparison
     * @param <T> type of the outer exception
     */
    public static <T extends Throwable> Matcher<T> rootCause(
            final Class<? extends Throwable> type,
            final String message) {
        return rootCause(allOf(instanceOf(type), exceptionMessage(message)));
    }

}
