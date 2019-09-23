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

package org.neo4j.graphalgo.core;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.CoreMatchers.is;

public final class ExceptionMessageMatcher<EX extends Throwable>
        extends TypeSafeMatcher<EX> {

    private final Matcher<? super String> matcher;

    private ExceptionMessageMatcher(final Matcher<String> matcher) {
        this.matcher = matcher;
    }

    public static <EX extends Throwable> Matcher<EX> exceptionMessage(final String message) {
        return new ExceptionMessageMatcher<>(is(message));
    }

    @Override
    protected boolean matchesSafely(final EX ex) {
        return matcher.matches(ex.getMessage());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendDescriptionOf(matcher);
    }

}

