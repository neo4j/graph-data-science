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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.ImmutableLinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionSimilarityComputerTest {
    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (a { prop1: 4,  prop2: [1L, 0L, 0L, 0L]})" +
        "  (b { prop1: 100,  prop2: [0L, 10L, 0L, 0L]})" +
        ", (c { prop1: 1, prop2: [2L, 1L, 0L, 0L]})"+
        ", (a)-->(a)" +
        ", (a)-->(b)"+
        ", (c)-->(a)";


    @Inject
    public TestGraph graph;

    @Test
    void validateComputeSimilarity() {
        var linkFeatureSteps = List.<LinkFeatureStep>of(
            new CosineFeatureStep(List.of("prop2")),
            new HadamardFeatureStep(List.of("prop1"))
        );
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, linkFeatureSteps);
        var modelData = ImmutableLinkLogisticRegressionData.of(
            new Weights<>(new Matrix(
                new double[]{1, 0.0001},
                1,
                2
            )),
            Weights.ofScalar(0)
        );
        var lpSimComputer = new LinkPredictionSimilarityComputer(
            linkFeatureExtractor,
            new LinkLogisticRegressionPredictor(modelData),
            graph
        );
        assertThat(lpSimComputer.similarity(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"))).isEqualTo(
            0.5099986668799655);
        assertThat(lpSimComputer.similarity(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"))).isEqualTo(
            0.7098853299317623);
    }

    @Test
    void filterExistingRelationships() {
        var linkFeatureSteps = List.<LinkFeatureStep>of();
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, linkFeatureSteps);
        var modelData = ImmutableLinkLogisticRegressionData.of(
            Weights.ofMatrix(1, 2),
            Weights.ofScalar(0)
        );
        var lpSimComputer = new LinkPredictionSimilarityComputer(
            linkFeatureExtractor,
            new LinkLogisticRegressionPredictor(modelData),
            graph
        );
        // The node filter does not support self-loops as a node is always similar to itself so a-->a should be false.
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("a"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"))).isEqualTo(true);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("a"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("b"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("a"))).isEqualTo(true);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("c"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"))).isEqualTo(false);
        assertThat(lpSimComputer.filterNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("b"))).isEqualTo(false);
    }
}
