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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.linkmodels.pipeline.predict.LPGraphStoreFilterFactory.generate;

@GdlExtension
class LPGraphFilterFactoryTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n0 :A {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n1: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n2: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n3: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n4: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n5: A {a: 1.0, b: 0.8, c: 1.0})," +
                                  "(n0)-[:T]->(n1), " +
                                  "(n1)-[:T]->(n2), " +
                                  "(n2)-[:T]->(n0), " +
                                  "(n5)-[:T]->(n1)," +
                                  "(n5)-[:OTHER]->(n1)," +
                                  "(n5)-[:CONTEXT_NEW]->(n1)," +
                                  "(n4)-[:CONTEXT]->(n0) ";

    @Inject
    GraphStore multiLabelGraphStore;

    LinkPredictionTrainConfig trainConfig = LinkPredictionTrainConfigImpl
        .builder()
        .pipeline("dummy")
        .graphName("dummy")
        .modelName("dummy")
        .modelUser("dummy")
        .targetRelationshipType("T")
        .sourceNodeLabel("A")
        .targetNodeLabel("B")
        .contextNodeLabels(List.of("C"))
        .contextRelationshipTypes(List.of("CONTEXT"))
        .build();

    @Test
    void generateFromTrainConfig() {
        var predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName("dummy")
            .graphName("dummy")
            .modelUser("dummy")
            .topN(42)
            .build();

        var labelFilter = generate(trainConfig, predictConfig, multiLabelGraphStore, ProgressTracker.NULL_TRACKER);

        assertThat(labelFilter.sourceNodeLabels()).containsExactly(NodeLabel.of("A"));
        assertThat(labelFilter.targetNodeLabels()).containsExactly(NodeLabel.of("B"));
        assertThat(labelFilter.nodePropertyStepsLabels()).containsExactlyInAnyOrder(
            NodeLabel.of("A"),
            NodeLabel.of("B"),
            NodeLabel.of("C")
        );
        assertThat(labelFilter.nodePropertyStepRelationshipTypes()).containsExactlyInAnyOrder(
            RelationshipType.of("T"),
            RelationshipType.of("CONTEXT")
        );
    }

    @Test
    void generateFromPredictConfig() {
        var predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName("dummy")
            .graphName("dummy")
            .modelUser("dummy")
            .sourceNodeLabel("B")
            .targetNodeLabel("A")
            .contextNodeLabels(List.of("A"))
            .contextRelationshipTypes(List.of("CONTEXT_NEW"))
            .relationshipTypes(List.of("OTHER"))
            .topN(42)
            .build();

        var filter = generate(trainConfig, predictConfig, multiLabelGraphStore, ProgressTracker.NULL_TRACKER);

        assertThat(filter.sourceNodeLabels()).containsExactly(NodeLabel.of("B"));
        assertThat(filter.targetNodeLabels()).containsExactly(NodeLabel.of("A"));
        assertThat(filter.nodePropertyStepsLabels()).containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));
        assertThat(filter.nodePropertyStepRelationshipTypes()).containsExactlyInAnyOrder(
            RelationshipType.of("OTHER"),
            RelationshipType.of("CONTEXT_NEW")
        );
    }

    @Test
    void failOnNodeSchemaMismatch() {
        String username = "myUser";
        String modelName = "myModel";

        var trainConfig = LinkPredictionTrainConfigImpl
            .builder()
            .pipeline("pipe")
            .graphName("graph")
            .modelName(modelName)
            .modelUser(username)
            .sourceNodeLabel("A")
            .targetNodeLabel("INVALID_2")
            .targetRelationshipType("T")
            .build();

        var predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName(modelName)
            .graphName("dummy")
            .modelUser(username)
            .contextNodeLabels(List.of("INVALID"))
            .topN(42)
            .build();

        // only validates train here as predict config is already validated in before
        assertThatThrownBy(() -> generate(
            trainConfig,
            predictConfig,
            multiLabelGraphStore,
            ProgressTracker.NULL_TRACKER
        ))
            .hasMessage("Could not find the specified `targetNodeLabel` from the model's train config of ['INVALID_2']. " +
                        "Available labels are ['A', 'B', 'C'].");
    }

    @Test
    void failOnRelationshipSchemaMismatch() {
        String username = "myUser";
        String modelName = "myModel";

        var trainConfig = LinkPredictionTrainConfigImpl
            .builder()
            .pipeline("pipe")
            .graphName("graph")
            .modelName(modelName)
            .modelUser(username)
            .sourceNodeLabel("A")
            .targetNodeLabel("B")
            .targetRelationshipType("T")
            .contextRelationshipTypes(List.of("INVALID"))
            .build();

        var predictConfig = LinkPredictionPredictPipelineBaseConfigImpl.builder()
            .modelName(modelName)
            .graphName("dummy")
            .modelUser(username)
            .relationshipTypes(List.of("T"))
            .topN(42)
            .build();

        // only validates train here as predict config is already validated in before
        assertThatThrownBy(() -> generate(
            trainConfig,
            predictConfig,
            multiLabelGraphStore,
            ProgressTracker.NULL_TRACKER
        ))
            .hasMessage("Could not find the specified `contextRelationshipTypes` from the model's train config of ['INVALID']. " +
                        "Available relationship types are ['CONTEXT', 'CONTEXT_NEW', 'OTHER', 'T'].");
    }

}
