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
package org.neo4j.gds;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.stream.Stream;

public final class GraphFactoryTestSupport {

    public enum FactoryType {
        NATIVE,
        CYPHER
    }

    private GraphFactoryTestSupport() {}

    public static Stream<FactoryType> allFactoryTypes() {
        return Stream.of(FactoryType.NATIVE, FactoryType.CYPHER);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.gds.GraphFactoryTestSupport#allFactoryTypes")
    public @interface AllGraphStoreFactoryTypesTest {}
}
