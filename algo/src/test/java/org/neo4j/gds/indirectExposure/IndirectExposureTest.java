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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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

    static Stream<Arguments> exposures() {
        return Stream.of(
            Arguments.of("MAX", Map.of(
                "e00", 1.0,
                "e11", 0.200,
                "e12", 0.154,
                "e13", 0.167,
                "e21", 0.200,
                "e22", 0.055,
                "e23", 0.090,
                "e24", 0.167,
                "e31", 0.026,
                "e41", 0.026
            )),
            Arguments.of("SUM", Map.of(
                "e00", 1.0,
                "e11", 0.200,
                "e12", 0.154,
                "e13", 0.167,
                "e21", 0.200,
                "e22", 0.055,
                "e23", 0.161,
                "e24", 0.167,
                "e31", 0.026,
                "e41", 0.026
            ))
        );
    }

    @ParameterizedTest(name = "Exposure reducer: {0}")
    @MethodSource("exposures")
    void compute(String exposureReducer, Map<String, Double> expectedExposures, SoftAssertions softly) {
        var config = IndirectExposureConfigImpl.builder()
            .concurrency(1)
            .maxIterations(5)
            .exposureReducer(exposureReducer)
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

        expectedExposures.forEach((nodeVariable, expectedExposure) -> softly
            .assertThat(exposures.get(graph.toMappedNodeId(nodeVariable)))
            .as(nodeVariable)
            .isCloseTo(expectedExposure, offset));
    }

}
