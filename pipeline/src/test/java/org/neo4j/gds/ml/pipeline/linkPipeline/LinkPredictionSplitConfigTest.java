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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class LinkPredictionSplitConfigTest {

    @GdlGraph
    static String GRAPH =
        " ()-->()" +
        StringUtils.repeat(" ,()-->()", 9);

    @Inject
    GraphStore graphStore;

    @GdlGraph(graphNamePrefix = "negative")
    static String negativeGraph =
        "(:A)-[:T]->(:A)";

    @Inject
    GraphStore negativeGraphStore;

    @Test
    void expectedSetSize() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.2)
            .trainFraction(0.2)
            .build();

        GraphDimensions inputDim = GraphDimensions
            .builder()
            .nodeCount(100)
            .putRelationshipCount(RelationshipType.of("TARGET"), 100)
            .putRelationshipCount(RelationshipType.of("CONTEXT"), 1000)
            .relCountUpperBound(1100)
            .build();

        GraphDimensions expectedDim = GraphDimensions.builder()
            .nodeCount(100)
            .putRelationshipCount(RelationshipType.of("TARGET"), 100)
            .putRelationshipCount(RelationshipType.of("CONTEXT"), 1000)
            .putRelationshipCount(config.testRelationshipType(), 20)
            .putRelationshipCount(config.testComplementRelationshipType(), 80)
            .putRelationshipCount(config.trainRelationshipType(), 16)
            .putRelationshipCount(config.featureInputRelationshipType(), 64)
            .relCountUpperBound(1100)
            .build();

        assertThat(config.expectedGraphDimensions(inputDim, "TARGET")).isEqualTo(expectedDim);
    }

    @Test
    void shouldThrowOnEmptyTestComplementSet() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.99)
            .trainFraction(0.01)
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(graphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("The specified `testFraction` is too high for the current graph. " +
                                  "The test-complement set would have 0 relationship(s) but it must have at least 3.");
    }

    @Test
    void shouldThrowOnEmptyTrainSet() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.7)
            .trainFraction(0.1)
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(graphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("The specified `trainFraction` is too low for the current graph. " +
                                  "The train set would have 0 relationship(s) but it must have at least 2.");
    }

    @Test
    void shouldThrowOnEmptyTestSet() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.01)
            .trainFraction(0.99)
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(graphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("The specified `testFraction` is too low for the current graph. " +
                                  "The test set would have 0 relationship(s) but it must have at least 1.");
    }

    @Test
    void shouldThrowOnEmptyFeatureInputSet() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.2)
            .trainFraction(0.99)
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(graphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("The specified `trainFraction` is too high for the current graph. " +
                                  "The feature-input set would have 0 relationship(s) but it must have at least 1.");
    }

    @Test
    void shouldThrowOnEmptyValidationFoldSet() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .validationFolds(100)
            .testFraction(0.2)
            .trainFraction(0.5)
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(graphStore, RelationshipType.ALL_RELATIONSHIPS, "A", "B"))
            .hasMessageContaining(
                "The specified `validationFolds` is too high or the `trainFraction` too low for the current graph. " +
                "The validation set would have 0 relationship(s) but it must have at least 1.");
    }

    @Test
    void shouldValidateNegativeRelationshipTypeExist() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .testFraction(0.5)
            .trainFraction(0.5)
            .negativeRelationshipType("N")
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(negativeGraphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("Could not find the specified negativeRelationshipType of ['N']. Available relationship types are ['T'].");
    }

    @Test
    void shouldValidateNegativeRelationshipTypeAndNegativeSamplingRatioAreExclusive() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .testFraction(0.5)
            .trainFraction(0.5)
            .negativeSamplingRatio(2.0)
            .negativeRelationshipType("T")
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(negativeGraphStore, RelationshipType.ALL_RELATIONSHIPS, "*", "*"))
            .hasMessageContaining("NegativeSamplingRatio and NegativeRelationshipType cannot be used together.");
    }

    @Test
    void shouldValidateNegativeRelationshipTypeRespectNodeLabels() {
        var config = LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .testFraction(0.5)
            .trainFraction(0.5)
            .negativeRelationshipType("T")
            .build();

        assertThatThrownBy(() -> config.validateAgainstGraphStore(negativeGraphStore, RelationshipType.ALL_RELATIONSHIPS, "A", "B"))
            .hasMessageContaining("There is a relationship of negativeRelationshipType T between nodes 0 and 1. " +
                                  "The nodes have types [NodeLabel{name='A'}] and [NodeLabel{name='A'}]. However, they need to be between A and B.");
    }
}
