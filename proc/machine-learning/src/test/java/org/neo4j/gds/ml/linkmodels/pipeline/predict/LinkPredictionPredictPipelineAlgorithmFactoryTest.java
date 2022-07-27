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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionPredictPipelineAlgorithmFactoryTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n0 :A {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n1: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n2: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n3: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n4: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n5: A {a: 1.0, b: 0.8, c: 1.0})" +
                                  "(n0)-[:T]->(n1), (n1)-[:T]->(n2), (n2)-[:T}->(n0), (n5)-[:T]->(n1)";


    @Inject
    TestGraph multiLabelGraph;

    @Inject
    GraphStore multiLabelGraphStore;

    LinkPredictionTrainConfig trainConfig = LinkPredictionTrainConfigImpl
        .builder()
        .pipeline("dummy")
        .graphName("dummy")
        .modelName("dummy")
        .username("dummy")
        .targetRelationshipType("T")
        .sourceNodeLabel("A")
        .targetNodeLabel("B")
        .contextNodeLabels(List.of("A", "B", "C"))
        .build();

    @Test
    void generateNodeLabelsFromTrainConfig() {
        LinkPredictionPredictPipelineBaseConfig predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName("dummy")
            .graphName("dummy")
            .username("dummy")
            .topN(42)
            .build();

        var nodeLabelFilterForPrediction = LinkPredictionPredictPipelineAlgorithmFactory.generateNodeLabels(trainConfig, predictConfig, multiLabelGraphStore);

        assertThat(nodeLabelFilterForPrediction.internalSourceNodeLabels()).containsExactly(NodeLabel.of("A"));
        assertThat(nodeLabelFilterForPrediction.internalTargetNodeLabels()).containsExactly(NodeLabel.of("B"));
        assertThat(nodeLabelFilterForPrediction.nodePropertyStepsLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"), NodeLabel.of("C"));

    }

    @Test
    void generateNodeLabelsFromPredictConfig() {
        LinkPredictionPredictPipelineBaseConfig predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName("dummy")
            .graphName("dummy")
            .username("dummy")
            .sourceNodeLabel("B")
            .targetNodeLabel("A")
            .contextNodeLabels(List.of("A"))
            .topN(42)
            .build();

        var nodeLabelFilterForPrediction = LinkPredictionPredictPipelineAlgorithmFactory.generateNodeLabels(trainConfig, predictConfig, multiLabelGraphStore);

        assertThat(nodeLabelFilterForPrediction.internalSourceNodeLabels()).containsExactly(NodeLabel.of("B"));
        assertThat(nodeLabelFilterForPrediction.internalTargetNodeLabels()).containsExactly(NodeLabel.of("A"));
        assertThat(nodeLabelFilterForPrediction.nodePropertyStepsLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));

    }
}