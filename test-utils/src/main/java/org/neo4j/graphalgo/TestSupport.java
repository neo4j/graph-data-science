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
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.stream.Stream;


public class TestSupport {

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.TestSupport#allTypesWithoutCypher")
    public @interface AllGraphTypesWithoutCypherTest {}

    public static Stream<Class<? extends GraphFactory>> allTypesWithoutCypher() {
        return Stream.of(
                GraphViewFactory.class,
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

    public static Stream<String> allGraphNames() {
        return Stream.of("heavy", "huge", "kernel");
    }

}
