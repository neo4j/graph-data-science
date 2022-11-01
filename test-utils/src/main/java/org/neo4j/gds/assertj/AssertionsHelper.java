/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.assertj;

import org.assertj.core.api.AbstractBooleanAssert;
import org.assertj.core.api.AbstractIntegerAssert;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.MapAssert;
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsMapContaining;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

public final class AssertionsHelper {

    private static final Pattern PROGRESS_PATTERN = Pattern.compile("(\\d+)(%)$");


    private AssertionsHelper() {}

    @SuppressWarnings({ "rawtypes" })
    public static InstanceOfAssertFactory<Map, MapAssert<String, Object>> stringObjectMapAssertFactory() {
        return InstanceOfAssertFactories.map(
            String.class,
            Object.class
        );
    }

    public static Consumer<Object> booleanAssertConsumer(Consumer<AbstractBooleanAssert<?>> assertion) {
        return actualValue ->
            assertion.accept(
                assertThat(actualValue)
                    .asInstanceOf(InstanceOfAssertFactories.BOOLEAN)
            );
    }

    public static Consumer<Object> creationTimeAssertConsumer() {
        return creationTime -> assertThat(creationTime).isInstanceOf(ZonedDateTime.class);
    }

    public static Consumer<Object> intAssertConsumer(Consumer<AbstractIntegerAssert<?>> assertion) {
        return actualValue ->
            assertion.accept(assertThat(actualValue)
                .asInstanceOf(InstanceOfAssertFactories.INTEGER));

    }

    public static Consumer<Object> longAssertConsumer(Consumer<AbstractLongAssert<?>> assertion) {
        return actualValue -> assertion.accept(assertThat(actualValue)
            .asInstanceOf(InstanceOfAssertFactories.LONG));
    }

    public static Consumer<Object> stringAssertConsumer(Consumer<AbstractStringAssert<?>> assertion) {
        return actualValue -> assertion.accept(
            assertThat(actualValue)
                .asInstanceOf(InstanceOfAssertFactories.STRING)
        );
    }

    @SuppressWarnings({ "rawtypes" })
    public static Consumer<Object> listAssertConsumer(Consumer<AbstractListAssert> assertion) {
        return actualValue -> assertion.accept(
            assertThat(actualValue)
                .asList()
        );
    }

    public static Matcher<Map<? extends String, ?>> hasEntry(String key, Object value) {
        return IsMapContaining.hasEntry(equalTo(key), equalTo(value));
    }

    public static Predicate<String> progressExceeding100Predicate() {
        return message -> {
                java.util.regex.Matcher matcher = PROGRESS_PATTERN.matcher(message);
                return matcher.find() && Integer.parseInt(matcher.group(1)) > 100;
        };
    }
}
