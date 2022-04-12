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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class NodePropertyPredictionSplitConfigTest {

    @GdlGraph
    static String GDL = "  (a:Node)" +
                        ", (b:Node)" +
                        ", (c:Node)" +
                        ", (d:Node)" +
                        ", (e:Node)" +
                        ", (f:Node)" +
                        ", (a)-[:R]->(b)" +
                        ", (b)-[:R]->(c)" +
                        ", (c)-[:R]->(a)";

    @Inject
    private Graph graph;

    @Test
    void shouldThrowOnEmptyTestSet() {
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.1)
            .validationFolds(2)
            .build();
        assertThatThrownBy(() -> splitConfig.validateMinNumNodesInSplitSets(graph)).hasMessageContaining(
            "The specified `testFraction` is too low for the current graph. " +
            "The test set would have 0 node(s) but it must have at least 1."
        );
    }

    @Test
    void shouldThrowOnTooSmallTrainSet() {
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.9)
            .validationFolds(2)
            .build();
        assertThatThrownBy(() -> splitConfig.validateMinNumNodesInSplitSets(graph)).hasMessageContaining(
            "The specified `testFraction` is too high for the current graph. " +
            "The train set would have 1 node(s) but it must have at least 2."
        );
    }

    @Test
    void shouldThrowOnEmptyValidationSet() {
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.5)
            .validationFolds(6)
            .build();
        assertThatThrownBy(() -> splitConfig.validateMinNumNodesInSplitSets(graph)).hasMessageContaining(
            "The specified `validationFolds` or `testFraction` is too high for the current graph. " +
            "The validation set would have 0 node(s) but it must have at least 1."
        );
    }
}
