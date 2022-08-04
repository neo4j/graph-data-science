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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPredictionSimilarityComputer.LinkFilterFactory;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.similarity.knn.NeighborFilter;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionSimilarityComputerTest {
    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (a: N { prop1: 4,  prop2: [1L, 0L, 0L, 0L]})" +
        "  (b: N { prop1: 100,  prop2: [0L, 10L, 0L, 0L]})" +
        ", (c: M { prop1: 1, prop2: [2L, 1L, 0L, 0L]})"+
        ", (a)-->(a)" +
        ", (a)-->(b)"+
        ", (c)-->(a)";


    @Inject
    public TestGraph graph;

    @Inject
    public GraphStore graphStore;

    @Test
    void validateComputeSimilarity() {
        var linkFeatureSteps = List.<LinkFeatureStep>of(
            new CosineFeatureStep(List.of("prop2")),
            new HadamardFeatureStep(List.of("prop1"))
        );
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, linkFeatureSteps);
        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(new Matrix(
                new double[]{-1, -0.0001},
                1,
                2
            )),
            Weights.ofVector(0.0)
        );
        var lpSimComputer = new LinkPredictionSimilarityComputer(
            linkFeatureExtractor,
            LogisticRegressionClassifier.from(modelData)
        );
        assertThat(lpSimComputer.similarity(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"))).isEqualTo(
            0.5099986668799655, Offset.offset(1e-9));
        assertThat(lpSimComputer.similarity(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"))).isEqualTo(
            0.7098853299317623, Offset.offset(1e-9));
    }

    @Test
    void filterExistingRelationships() {
        var nodeFilter = LPNodeFilter.of(graph, graph);
        NeighborFilter filter = new LinkFilterFactory(graph, nodeFilter, nodeFilter).create();

        // The node filter does not support self-loops as a node is always similar to itself so a-->a should be false.
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("a"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("a"))).isEqualTo(false);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("b"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("a"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("c"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"))).isEqualTo(false);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"))).isEqualTo(false);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("b"))).isEqualTo(false);
    }

    @Test
    void filterNodeLabelsAndExistingRelationships() {
        var nodeFilter = LPNodeFilter.of(graph, graphStore.getGraph(NodeLabel.of("N")));
        NeighborFilter filter = new LinkFilterFactory(graph, nodeFilter, nodeFilter).create();

        // The node filter does not support self-loops as a node is always similar to itself so a-->a should be false.
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("a"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("a"))).isEqualTo(false);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("b"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("a"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("c"))).isEqualTo(true);
        //C has nodeLabel M which should doesn't satisfy source/target nodeLabel filter, exclude edges on C
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"))).isEqualTo(true);
        assertThat(filter.excludeNodePair(graph.toMappedNodeId("c"), graph.toMappedNodeId("b"))).isEqualTo(true);
    }

    @Test
    void neighbourEstimates() {
        var nodeFilter = LPNodeFilter.of(graph, graph);
        NeighborFilter filter = new LinkFilterFactory(graph, nodeFilter, nodeFilter).create();

        assertThat(filter.lowerBoundOfPotentialNeighbours(graph.toMappedNodeId("a")))
            .isLessThanOrEqualTo(1)
            .isGreaterThanOrEqualTo(0);
        assertThat(filter.lowerBoundOfPotentialNeighbours(graph.toMappedNodeId("b")))
            .isLessThanOrEqualTo(2)
            .isGreaterThanOrEqualTo(0);
        assertThat(filter.lowerBoundOfPotentialNeighbours(graph.toMappedNodeId("c")))
            .isLessThanOrEqualTo(1)
            .isGreaterThanOrEqualTo(0);
    }
}
