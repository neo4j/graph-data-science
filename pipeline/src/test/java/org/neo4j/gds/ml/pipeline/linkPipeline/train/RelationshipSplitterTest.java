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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.assertj.Extractors;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.RelationshipSplitter.progressTask;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.RelationshipSplitter.splitEstimation;

@GdlExtension
class RelationshipSplitterTest {

    @GdlGraph
    private static final String GRAPH =
        "CREATE " +
        "(a:N {scalar: 0, array: [-1.0, -2.0, 1.0, 1.0, 3.0]}), " +
        "(b:N {scalar: 4, array: [2.0, 1.0, -2.0, 2.0, 1.0]}), " +
        "(c:N {scalar: 0, array: [-3.0, 4.0, 3.0, 3.0, 2.0]}), " +
        "(d:N {scalar: 3, array: [1.0, 3.0, 1.0, -1.0, -1.0]}), " +
        "(e:N {scalar: 1, array: [-2.0, 1.0, 2.0, 1.0, -1.0]}), " +
        "(f:N {scalar: 0, array: [-1.0, -3.0, 1.0, 2.0, 2.0]}), " +
        "(g:N {scalar: 1, array: [3.0, 1.0, -3.0, 3.0, 1.0]}), " +
        "(h:N {scalar: 3, array: [-1.0, 3.0, 2.0, 1.0, -3.0]}), " +
        "(i:N {scalar: 3, array: [4.0, 1.0, 1.0, 2.0, 1.0]}), " +
        "(j:N {scalar: 4, array: [1.0, -4.0, 2.0, -2.0, 2.0]}), " +
        "(k:N {scalar: 0, array: [2.0, 1.0, 3.0, 1.0, 1.0]}), " +
        "(l:N {scalar: 1, array: [-1.0, 3.0, -2.0, 3.0, -2.0]}), " +
        "(m:N {scalar: 0, array: [4.0, 4.0, 1.0, 1.0, 1.0]}), " +
        "(n:N {scalar: 3, array: [1.0, -2.0, 3.0, 2.0, 3.0]}), " +
        "(o:N {scalar: 2, array: [-3.0, 3.0, -1.0, -1.0, 1.0]}), " +
        "" +
        "(a)-[:REL {weight: 2.0}]->(b), " +
        "(a)-[:REL {weight: 1.0}]->(c), " +
        "(b)-[:REL {weight: 3.0}]->(c), " +
        "(c)-[:REL {weight: 4.0}]->(d), " +
        "(e)-[:REL {weight: 5.0}]->(f), " +
        "(f)-[:REL {weight: 2.0}]->(g), " +
        "(h)-[:REL {weight: 2.0}]->(i), " +
        "(j)-[:REL {weight: 2.0}]->(k), " +
        "(k)-[:REL {weight: 2.0}]->(l), " +
        "(m)-[:REL {weight: 2.0}]->(n), " +
        "(n)-[:REL {weight: 4.0}]->(o), " +
        "(a)-[:REL {weight: 2.0}]->(d), " +
        "(b)-[:REL {weight: 2.0}]->(d), " +
        "(e)-[:REL {weight: 0.5}]->(g), " +
        "(j)-[:REL {weight: 2.0}]->(l), " +
        "(m)-[:REL {weight: 2.0}]->(o), " +
        "(a)-[:NEGATIVE]->(k), " +
        "(b)-[:NEGATIVE]->(k), " +
        "(c)-[:NEGATIVE]->(k)";

    @Inject
    GraphStore graphStore;

    @GdlGraph(graphNamePrefix = "multi")
    private static final String MULTI_GRAPH =
        "CREATE " +
        "(n1:N), " +
        "(n2:N), " +
        "(n3:N), " +
        "(m1:M), " +
        "(m2:M), " +
        "(m3:M), " +
        "" +
        "(n1)-[:T]->(n2), " +
        "(n2)-[:T]->(n3), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +
        "(n3)-[:T]->(n2), " +

        "(n1)-[:T]->(m1)";

    @Inject
    GraphStore multiGraphStore;

    @Test
    void splitWeightedGraph() {
        var splitConfig = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.3)
            .testFraction(0.3)
            .validationFolds(2)
            .negativeSamplingRatio(1.0)
            .build();

        RelationshipSplitter relationshipSplitter = new RelationshipSplitter(
            graphStore,
            splitConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        relationshipSplitter.splitRelationships(RelationshipType.of("REL"), "N", "N", Optional.of(42L), Optional.of("weight"));

        var expectedRelTypes = Stream.of(
                splitConfig.trainRelationshipType(),
                splitConfig.testRelationshipType(),
                splitConfig.featureInputRelationshipType(),
                RelationshipType.of("REL"),
                RelationshipType.of("NEGATIVE")
            )
            .collect(Collectors.toList());

        assertThat(graphStore.relationshipTypes()).containsExactlyInAnyOrderElementsOf(expectedRelTypes);

        var expectedRelProperties = Map.of(
            splitConfig.featureInputRelationshipType(), Set.of("weight"),
            splitConfig.trainRelationshipType(), Set.of("label"),
            splitConfig.testRelationshipType(), Set.of("label"),
            RelationshipType.of("REL"), Set.of("weight"),
            RelationshipType.of("NEGATIVE"), Set.of()
        );

        Map<RelationshipType, Set<String>> actualRelProperties = graphStore
            .schema()
            .relationshipSchema()
            .properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, i -> i.getValue().keySet()));

        assertThat(actualRelProperties).usingRecursiveComparison().isEqualTo(expectedRelProperties);
    }

    @Test
    void warnForUnfilteredSplitting() {
        var splitConfig = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.3)
            .testFraction(0.3)
            .validationFolds(2)
            .negativeSamplingRatio(1.0)
            .build();

        var progressTracker = new InspectableTestProgressTracker(progressTask(splitConfig.expectedSetSizes(graphStore.relationshipCount())), "user", new JobId());

        var relationshipSplitter = new RelationshipSplitter(
            graphStore,
            splitConfig,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );

        relationshipSplitter.splitRelationships(RelationshipType.of("REL"), "*", "N", Optional.of(42L), Optional.of("weight"));

        assertThat(progressTracker.log().getMessages(TestLog.WARN))
            .extracting(Extractors.removingThreadId())
            .contains("Split relationships :: Using * for the `sourceNodeLabel` or `targetNodeLabel` results in not ideal negative link sampling.");
    }

    @Test
    void estimateWithDifferentTestFraction() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.3)
            .validationFolds(3)
            .negativeSamplingRatio(1.0);

        var splitConfig = splitConfigBuilder.testFraction(0.2).build();
        var actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(28_800, 35_840));

        splitConfig = splitConfigBuilder.testFraction(0.8).build();
        actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        // higher testFraction -> lower estimation as test-complement is smaller
        // the test_complement is kept until the end of all splitting
        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(19_152, 32_912));
    }

    @Test
    void estimateWithDifferentTrainFraction() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(3)
            .negativeSamplingRatio(1.0);

        var splitConfig = splitConfigBuilder.trainFraction(0.2).build();
        var actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27_200, 34_240));

        splitConfig = splitConfigBuilder.trainFraction(0.8).build();
        actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27_184, 40_944));
    }

    @Test
    void estimateWithDifferentNegativeSampling() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .trainFraction(0.3)
            .validationFolds(3);

        var splitConfig = splitConfigBuilder.negativeSamplingRatio(1).build();
        var actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27184, 35_344));

        splitConfig = splitConfigBuilder.negativeSamplingRatio(4).build();
        actualEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(39424, 59_824));
    }

    @Test
    void estimateWithDifferentRelationshipWeight() {
        var splitConfig = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .trainFraction(0.3)
            .validationFolds(3).negativeSamplingRatio(1).build();
        var unweightedEstimation = splitEstimation(splitConfig, "REL", Optional.empty(), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        var weightedEstimation = splitEstimation(splitConfig, "REL", Optional.of("weight"), "N", "N")
            .estimate(splitConfig.expectedGraphDimensions(GraphDimensions.of(100, 1_000), "REL"), 4);

        assertThat(unweightedEstimation.memoryUsage()).isNotEqualTo(weightedEstimation.memoryUsage());
    }

    @Test
    void failOnSmallFilteredGraph() {
        var splitConfig = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.3)
            .testFraction(0.3)
            .validationFolds(2)
            .negativeSamplingRatio(1.0)
            .build();

        RelationshipSplitter relationshipSplitter = new RelationshipSplitter(
            multiGraphStore,
            splitConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        // due to the label filter most of the relationships are not valid
        assertThatThrownBy(() -> relationshipSplitter.splitRelationships(RelationshipType.of("T"), "N", "M", Optional.of(42L), Optional.empty()))
            .hasMessageContaining("The specified `testFraction` is too low for the current graph. The test set would have 0 relationship(s) but it must have at least 1.");
    }

    @Test
    void splitWithSpecifiedNegativeRelationships() {
        var splitConfig = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.5)
            .testFraction(0.5)
            .validationFolds(2)
            .negativeSamplingRatio(99.0)
            .negativeRelationshipType("NEGATIVE")
            .build();

        var relationshipSplitter = new RelationshipSplitter(
            graphStore,
            splitConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        relationshipSplitter.splitRelationships(RelationshipType.of("REL"), "*", "N", Optional.of(42L), Optional.of("weight"));

        var testGraphSize = graphStore.relationshipCount(splitConfig.testRelationshipType());
        var trainGraphSize = graphStore.relationshipCount(splitConfig.trainRelationshipType());
        var featureInputGraphSize = graphStore.relationshipCount(splitConfig.featureInputRelationshipType());

        assertThat(testGraphSize).isEqualTo(10);
        assertThat(trainGraphSize).isEqualTo(5);
        assertThat(featureInputGraphSize).isEqualTo(7);

    }
}
