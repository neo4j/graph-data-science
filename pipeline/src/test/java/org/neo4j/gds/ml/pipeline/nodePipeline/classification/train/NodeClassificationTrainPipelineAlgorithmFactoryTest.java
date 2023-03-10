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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
public class NodeClassificationTrainPipelineAlgorithmFactoryTest {

    @GdlGraph
    private static final String REL_GRAPH_QUERY =
        "CREATE " +
        "  (b1:M {scalar: 1.2, array: [1.0, -1.0], t: 0})" +
        ", (b2:M {scalar: 0.5, array: [1.0, -1.0], t: 0})" +
        ", (b3:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b4:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b5:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b6:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b7:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b8:M {scalar: 1.1, array: [1.0, -1.0], t: 0})" +
        ", (b1)-[:R]->(b2)" +
        ", (b2)-[:R]->(b3)" +
        ", (b3)-[:R]->(b1)";

    @Inject
    private GraphStore graphStore;

    @Test
    void shouldValidateContextConfigsForNodePropertySteps() {
        var ncAlgoFactory = new NodeClassificationTrainPipelineAlgorithmFactory(ExecutionContext.EMPTY, "dummyVersion");
        var ncTrainConfig = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .graphName("g")
            .modelUser("DUMMY")
            .modelName("modelName")
            .concurrency(1)
            .randomSeed(42L)
            .targetNodeLabels(List.of("M"))
            .relationshipTypes(List.of("R"))
            .targetProperty("t")
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED")))
            .build();
        var ncTrainPipeline = new NodeClassificationTrainingPipeline();
        ncTrainPipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr", "contextNodeLabels", List.of("INVALID"))
        ));

        assertThatThrownBy(() -> ncAlgoFactory.build(graphStore, ncTrainConfig, ncTrainPipeline, ProgressTracker.NULL_TRACKER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not find the specified contextNodeLabels for step `gds.testProc.mutate` of ['INVALID']. Available labels are ['M'].");

        var ncTrainPipeline2 = new NodeClassificationTrainingPipeline();
        ncTrainPipeline2.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr", "contextRelationshipTypes", List.of("INVALID"))
        ));

        assertThatThrownBy(() -> ncAlgoFactory.build(graphStore, ncTrainConfig, ncTrainPipeline2, ProgressTracker.NULL_TRACKER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not find the specified contextRelationshipTypes for step `gds.testProc.mutate` of ['INVALID']. Available relationship types are ['R'].");

    }

}
