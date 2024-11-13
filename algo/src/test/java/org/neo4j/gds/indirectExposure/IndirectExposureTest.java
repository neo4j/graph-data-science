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
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.indirectExposure.IndirectExposureComputation.DEFAULT_EXPOSURE;
import static org.neo4j.gds.indirectExposure.IndirectExposureComputation.UNDEFINED;

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
        (e42)
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
        var expectedResults = new HashMap<String, List<Object>>();
        // Node, [Exposure, Hop, Parent, Root]
        expectedResults.put("e00", List.of(1.0, 0L, "e00", "e00"));
        expectedResults.put("e11", List.of(0.200, 1L, "e00", "e00"));
        expectedResults.put("e12", List.of(0.154, 1L, "e00", "e00"));
        expectedResults.put("e13", List.of(0.167, 1L, "e00", "e00"));
        expectedResults.put("e21", List.of(0.200, 2L, "e11", "e00"));
        expectedResults.put("e22", List.of(0.055, 2L, "e12", "e00"));
        expectedResults.put("e23", List.of(0.090, 2L, "e13", "e00"));
        expectedResults.put("e24", List.of(0.167, 2L, "e13", "e00"));
        expectedResults.put("e31", List.of(0.026, 3L, "e22", "e00"));
        expectedResults.put("e41", List.of(0.026, 4L, "e31", "e00"));
        // disconnected and not sanctioned, should get default values
        expectedResults.put("e42", List.of(
            DEFAULT_EXPOSURE.doubleValue(),
            UNDEFINED.longValue(),
            UNDEFINED.longValue(),
            UNDEFINED.longValue()
        ));

        var config = IndirectExposureConfigImpl.builder()
            .concurrency(1)
            .maxIterations(5)
            .sanctionedProperty("sanctioned")
            .relationshipWeightProperty("w")
            .build();

        var result = new IndirectExposure(
            graph,
            config,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER
        ).compute();

        var offset = Offset.offset(0.001);

        assertThat(result.didConverge()).isFalse();
        assertThat(result.iterations()).isEqualTo(5);

        var exposures = result.exposures();
        var hops = result.hops();
        var parents = result.parents();
        var roots = result.roots();

        var nodeVars = List.of("e00", "e11", "e12", "e13", "e21", "e22", "e23", "e24", "e31", "e41", "e42");

        nodeVars.forEach(nodeVar -> {
            var nodeId = graph.toMappedNodeId(nodeVar);
            var expected = expectedResults.get(nodeVar);
            var expectedExposure = (Double) expected.get(0);
            var expectedHop = (Long) expected.get(1);

            long expectedParent = getId(expected.get(2));
            long expectedRoot = getId(expected.get(3));

            softly.assertThat(exposures.get(nodeId)).as(nodeVar).isCloseTo(expectedExposure, offset);
            softly.assertThat(hops.get(nodeId)).as(nodeVar).isEqualTo(expectedHop);
            softly.assertThat(parents.get(nodeId)).as(nodeVar).isEqualTo(expectedParent);
            softly.assertThat(roots.get(nodeId)).as(nodeVar).isEqualTo(expectedRoot);
        });
    }

    private long getId(Object idObject) {
        if (idObject instanceof String s) {
            return this.graph.toOriginalNodeId(s);
        }
        if (idObject instanceof Long l) {
            return l;
        }
        throw new IllegalArgumentException("Expected id must be either String or Long");
    }
}
