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
package org.neo4j.gds.indirectExposure;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class IndirectExposureTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 42)
    public static final String G = """
        (e00 { sanctioned: 1L })
        (e11)
        (e12)
        (e13)
        (e21)
        (e22)
        (e23)
        (e24)
        (e31)
        (e41)
        (e00)-[:TRANSFER { w:  10 }]->(e11)
        (e00)-[:TRANSFER { w:  20 }]->(e12)
        (e00)-[:TRANSFER { w:  30 }]->(e13)
        (e11)-[:TRANSFER { w:  40 }]->(e21)
        (e12)-[:TRANSFER { w:  50 }]->(e22)
        (e12)-[:TRANSFER { w:  60 }]->(e23)
        (e13)-[:TRANSFER { w:  70 }]->(e23)
        (e13)-[:TRANSFER { w:  80 }]->(e24)
        (e22)-[:TRANSFER { w:  90 }]->(e31)
        (e31)-[:TRANSFER { w: 100 }]->(e41)
        """;

    @Inject
    private TestGraph graph;

    @Test
    void compute(SoftAssertions softly) {
        var config = IndirectExposureConfigImpl.builder()
            .concurrency(1)
            .maxIterations(5)
            .sanctionedProperty("sanctioned")
            .relationshipWeightProperty("w")
            .build();

        var result = new IndirectExposureFactory()
            .build(graph, config, ProgressTracker.NULL_TRACKER)
            .compute();

        var offset = Offset.offset(0.001);

        softly.assertThat(result.get(graph.toMappedNodeId("e00"))).as("e00").isEqualTo(1.0);
        softly.assertThat(result.get(graph.toMappedNodeId("e11"))).as("e11").isCloseTo(0.200, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e12"))).as("e12").isCloseTo(0.154, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e13"))).as("e13").isEqualTo(0.167, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e21"))).as("e21").isEqualTo(0.200, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e22"))).as("e22").isEqualTo(0.055, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e23"))).as("e23").isEqualTo(0.090, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e24"))).as("e24").isEqualTo(0.167, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e31"))).as("e31").isEqualTo(0.026, offset);
        softly.assertThat(result.get(graph.toMappedNodeId("e41"))).as("e41").isEqualTo(0.026, offset);
    }

}
