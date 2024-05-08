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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;

class TriangleCountMaxDegreeTest {

    @Test
    void shouldWorkWithMaxDegree() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(24)
            .averageDegree(23)
            .seed(101)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .direction(Direction.UNDIRECTED)
            .build()
            .generate();

        var tc = IntersectingTriangleCount.create(graph, new Concurrency(4), 100, DefaultPool.INSTANCE, ProgressTracker.NULL_TRACKER).compute();
        assertThat(tc.globalTriangles()).isEqualTo(1262L);
    }
}
