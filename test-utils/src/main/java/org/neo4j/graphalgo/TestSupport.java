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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphdb.Direction;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestSupport {

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.TestSupport#allTypesWithoutCypher")
    public @interface AllGraphTypesWithoutCypherTest {}

    // TODO: This isn't "parameterized" once kernel graph is gone
    public static Stream<Class<? extends GraphFactory>> allTypesWithoutCypher() {
        return Stream.of(
                HugeGraphFactory.class
        );
    }

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest(name = "graph: {0}")
    @MethodSource({"org.neo4j.graphalgo.TestSupport#allTypesWithoutCypher", "org.neo4j.graphalgo.TestSupport#cypherType"})
    public @interface AllGraphTypesTest {}

    public static Stream<Class<? extends GraphFactory>> cypherType() {
        return Stream.of(CypherGraphFactory.class);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.TestSupport#allGraphNames")
    public @interface AllGraphNamesTest {}

    // TODO: This isn't "parameterized" once kernel graph is gone
    public static Stream<String> allGraphNames() {
        return Stream.of("huge");
    }

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest(name = "parallel: {0}, graph: {1}")
    @MethodSource({"org.neo4j.graphalgo.TestSupport#singleAndMultiThreadedGraphNames"})
    public @interface SingleAndMultiThreadedAllGraphNames {}

    public static Stream<Arguments> singleAndMultiThreadedGraphNames() {
        return Stream.of(true, false).flatMap(parallel -> allGraphNames().map(name -> arguments(parallel, name)));
    }

    public static Stream<String> allDirectionsNames() {
        return Arrays.stream(Direction.values()).map(Direction::name);
    }

    public static Stream<Arguments> allGraphNamesAndDirections() {
        return allGraphNames().flatMap(name -> allDirectionsNames().map(direction -> arguments(name, direction)));
    }

}
