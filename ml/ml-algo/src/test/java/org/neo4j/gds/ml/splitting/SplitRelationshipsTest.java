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
package org.neo4j.gds.ml.splitting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;

@GdlExtension
class SplitRelationshipsTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 42)
    static final String GRAPH_WITH_OFFSET =
        "        CREATE" +
        "        (a: Node)," +
        "        (b: Node)," +
        "        (c: Node)," +
        "        (d: Node)," +
        "        (e: Node)," +
        "        (f: Node)," +
        "        (g: Node)," +
        "        (a)-[:REL]->(b)," +
        "        (a)-[:REL]->(c)," +
        "        (a)-[:REL]->(d)," +
        "        (a)-[:REL]->(e)," +
        "        (a)-[:REL]->(f)," +
        "        (a)-[:REL]->(g)," +
        "        (b)-[:REL]->(c)," +
        "        (b)-[:REL]->(d)," +
        "        (b)-[:REL]->(e)," +
        "        (b)-[:REL]->(f)," +
        "        (b)-[:REL2]->(g)," +
        "        (c)-[:REL2]->(d)," +
        "        (c)-[:REL2]->(e)," +
        "        (c)-[:REL2]->(f)," +
        "        (c)-[:REL2]->(g)";

    @Inject
    GraphStore graphStore;

    @Test
    void computeWithOffset() {
        var config = SplitRelationshipsBaseConfigImpl.builder()
            .holdoutFraction(0.2)
            .negativeSamplingRatio(1.0)
            .relationshipTypes(List.of("REL", "REL2"))
            .holdoutRelationshipType("TEST")
            .remainingRelationshipType("REST")
            .randomSeed(1337L)
            .build();

        SplitRelationships splitter = SplitRelationships.of(graphStore, config);

        EdgeSplitter.SplitResult result = splitter.compute();

        assertThat(result.selectedRels().build().relationships().topology().elementCount()).isEqualTo(6);
        assertThat(result.remainingRels().build().relationships().topology().elementCount()).isEqualTo(24);
    }

    @Test
    void estimate() {
        var graphDimensions = GraphDimensions.of(1, 10_000);
        var config = SplitRelationshipsMutateConfigImpl
            .builder()
            .negativeSamplingRatio(1.0)
            .holdoutRelationshipType("HOLDOUT")
            .remainingRelationshipType("REST")
            .holdoutFraction(0.3)
            .relationshipTypes(List.of(ElementProjection.PROJECT_ALL))
            .build();

        MemoryTree actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 208_000));
    }

    public static Stream<Arguments> withTypesParams() {
        return Stream.of(
            Arguments.of(List.of("TYPE1"), MemoryRange.of(160, 208)),
            Arguments.of(List.of("TYPE2"), MemoryRange.of(320, 416)),
            Arguments.of(List.of("*"), MemoryRange.of(960, 1_248)),
            Arguments.of(List.of("TYPE1", "TYPE2", "TYPE3"), MemoryRange.of(960, 1_248)),
            Arguments.of(List.of("TYPE1", "TYPE3"), MemoryRange.of(640, 832))
        );
    }

    @ParameterizedTest
    @MethodSource("withTypesParams")
    void estimateWithTypes(List<String> relTypes, MemoryRange expectedMemory) {
        var nodeCount = 100;
        var relationshipCounts = Map.of(
            RelationshipType.of("TYPE1"), 10L,
            RelationshipType.of("TYPE2"), 20L,
            RelationshipType.of("TYPE3"), 30L
        );

        var graphDimensions = ImmutableGraphDimensions.builder()
            .nodeCount(nodeCount)
            .relationshipCounts(relationshipCounts)
            .relCountUpperBound(relationshipCounts.values().stream().mapToLong(Long::longValue).sum())
            .build();

        var config = SplitRelationshipsMutateConfigImpl
            .builder()
            .negativeSamplingRatio(1.0)
            .holdoutRelationshipType("HOLDOUT")
            .remainingRelationshipType("REST")
            .holdoutFraction(0.3)
            .relationshipTypes(relTypes)
            .build();

        MemoryTree actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), expectedMemory);
    }

    @Test
    void estimateIndependentOfNodeCount() {
        var config = SplitRelationshipsMutateConfigImpl
            .builder()
            .negativeSamplingRatio(1.0)
            .holdoutRelationshipType("HOLDOUT")
            .remainingRelationshipType("REST")
            .holdoutFraction(0.3)
            .relationshipTypes(List.of(ElementProjection.PROJECT_ALL))
            .build();

        var graphDimensions = GraphDimensions.of(1, 10_000);
        MemoryTree actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());
        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 208_000));

        graphDimensions = GraphDimensions.of(100_000, 10_000);
        actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());
        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 208_000));
    }

    @Test
    void estimateDifferentSamplingRatios() {
        var graphDimensions = GraphDimensions.of(1, 10_000);

        var configBuilder = SplitRelationshipsMutateConfigImpl
            .builder()
            .holdoutRelationshipType("HOLDOUT")
            .remainingRelationshipType("REST")
            .holdoutFraction(0.3)
            .relationshipTypes(List.of(ElementProjection.PROJECT_ALL));

        var config = configBuilder.negativeSamplingRatio(1.0).build();
        MemoryTree actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 208_000));

        config = configBuilder.negativeSamplingRatio(2.0).build();
        actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(184_000, 256_000));
    }

    @Test
    void estimateDifferentHoldoutFractions() {
        var graphDimensions = GraphDimensions.of(1, 10_000);

        var configBuilder = SplitRelationshipsMutateConfigImpl
            .builder()
            .holdoutRelationshipType("HOLDOUT")
            .remainingRelationshipType("REST")
            .negativeSamplingRatio(1.0)
            .relationshipTypes(List.of(ElementProjection.PROJECT_ALL));

        var config = configBuilder.holdoutFraction(0.3).build();
        MemoryTree actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());
        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 208_000));

        config = configBuilder.holdoutFraction(0.1).build();
        actualEstimate = SplitRelationships.estimate(config)
            .estimate(graphDimensions, config.concurrency());

        assertMemoryRange(actualEstimate.memoryUsage(), MemoryRange.of(160_000, 176_000));
    }
}
