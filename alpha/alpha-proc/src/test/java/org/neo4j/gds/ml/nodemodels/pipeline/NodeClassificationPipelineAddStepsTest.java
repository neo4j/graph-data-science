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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.DEFAULT_PARAM_CONFIG;
import static org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationSplitConfig.DEFAULT_CONFIG;

class NodeClassificationPipelineAddStepsTest extends BaseProcTest {

    @BeforeEach
    void setUp() {
        NodeClassificationPipelineCreate.create(getUsername(), "myPipeline");
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void shouldAddNodePropertyStep() {
        var result = NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr")
        );
        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.splitConfig).isEqualTo(DEFAULT_CONFIG.toMap());
        assertThat(result.nodePropertySteps).isEqualTo(List.of(
            Map.of(
                "name", "gds.pageRank.mutate",
                "config", Map.of("mutateProperty", "pr")
            )));
        assertThat(result.featureProperties).isEqualTo(List.of());
        assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
    }

    @Test
    void shouldSelectFeatures() {
        NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "myPipeline",
            "test"
        );
        var result = NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "myPipeline",
            List.of("pr", "pr2")
        );
        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.splitConfig).isEqualTo(DEFAULT_CONFIG.toMap());
        assertThat(result.nodePropertySteps).isEqualTo(List.of());
        assertThat(result.featureProperties).isEqualTo(List.of("test", "pr", "pr2"));
        assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
    }

    @Test
    void failOnIncompleteNodePropertyStepConfig() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "fastRP",
            Map.of()
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Multiple errors in configuration arguments:")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `embeddingDimension`")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `mutateProperty`");
    }

    @Test
    void failOnDuplicateMutateProperty() {
        NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr")
        );
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The value of `mutateProperty` is expected to be unique, but pr was already specified in the gds.pageRank.mutate procedure.");
    }

    @Test
    void failOnUnexpectedConfigKeysInNodePropertyStepConfig() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr", "destroyEverything", true)
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unexpected configuration key: destroyEverything");
    }

    @Test
    void shouldAddNodeAndSelectFeatureProperties() {
        NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr")
        );
        NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "myPipeline",
            "pr"
        );
        var result = NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "myPipeline",
            "pr2"
        );
        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.splitConfig).isEqualTo(DEFAULT_CONFIG.toMap());
        assertThat(result.nodePropertySteps).isEqualTo(List.of(
            Map.of(
                "name", "gds.pageRank.mutate",
                "config", Map.of("mutateProperty", "pr")
            )));
        assertThat(result.featureProperties).isEqualTo(List.of("pr", "pr2"));
        assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
    }

    @Test
    void shouldAddTwoNodePropertySteps() {
        NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr")
        );
        var result = NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr2")
        );
        assertThat(result.name).isEqualTo("myPipeline");
        assertThat(result.splitConfig).isEqualTo(DEFAULT_CONFIG.toMap());
        assertThat(result.nodePropertySteps).isEqualTo(List.of(
                Map.of(
                    "name", "gds.pageRank.mutate",
                    "config", Map.of("mutateProperty", "pr")
                ),
                Map.of(
                    "name", "gds.pageRank.mutate",
                    "config", Map.of("mutateProperty", "pr2")
                )
            )
        );
        assertThat(result.featureProperties).isEqualTo(List.of());
        assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForNodePropertyStep() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "ceci n'est pas une pipe",
            "pageRank",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Pipeline with name `ceci n'est pas une pipe` does not exist for user ``.");
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForFeatureStep() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "ceci n'est pas une pipe",
            "test"
        ))
            .isExactlyInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Pipeline with name `ceci n'est pas une pipe` does not exist for user ``.");
    }

    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "juggleSpoons",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not find a procedure called gds.jugglespoons.mutate");
    }

    @Test
    void failOnConfiguringReservedConfigFields() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "myPipeline",
            "pageRank",
            Map.of("nodeLabels", List.of("LABEL"), "mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "Cannot configure ['nodeLabels', 'relationshipTypes'] for an individual node property step, but can only be configured at `train` and `predict` mode."
            );
    }

    @Test
    void shouldThrowIfAddingNodePropertyToANonPipeline() {
        PipelineCatalog.set(getUsername(), "testPipe", new LinkPredictionPipeline());

        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            "testPipe",
            "pageRank",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The pipeline `testPipe` is of type `Link prediction training pipeline`, but expected type `Node classification training pipeline`."
            );
    }

    @Test
    void shouldThrowIfAddingFeatureToANonPipeline() {
        PipelineCatalog.set(getUsername(), "testPipe", new LinkPredictionPipeline());

        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.selectFeatures(
            getUsername(),
            "testPipe",
            "something"
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The pipeline `testPipe` is of type `Link prediction training pipeline`, but expected type `Node classification training pipeline`.");
    }
}
