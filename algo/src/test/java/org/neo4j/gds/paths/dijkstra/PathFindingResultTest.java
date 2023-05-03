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
package org.neo4j.gds.paths.dijkstra;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.paths.ImmutablePathResult;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PathFindingResultTest {

    private Supplier<PathFindingResult> pathFindingResultSupplier;
    private MutableBoolean actionCalled;

    @BeforeEach
    void setup() {
        var pathResult = ImmutablePathResult.builder()
            .index(0)
            .sourceNode(0)
            .targetNode(1)
            .nodeIds(0, 1)
            .relationshipIds(0)
            .costs(1.0D)
            .build();
        actionCalled = new MutableBoolean(false);
        pathFindingResultSupplier = () -> new PathFindingResult(Stream.of(pathResult), actionCalled::setTrue);
    }

    @Test
    void testFindFirst() {
        pathFindingResultSupplier.get().findFirst();
        assertThat(actionCalled.booleanValue()).isTrue();
    }

    @Test
    void testForEachPath() {
        pathFindingResultSupplier.get().forEachPath(pathResult -> {});
        assertThat(actionCalled.booleanValue()).isTrue();
    }

    @Test
    void testMapPathWithClose() {
        pathFindingResultSupplier.get().mapPaths(pathResult -> pathResult).close();
        assertThat(actionCalled.booleanValue()).isTrue();
    }
}
