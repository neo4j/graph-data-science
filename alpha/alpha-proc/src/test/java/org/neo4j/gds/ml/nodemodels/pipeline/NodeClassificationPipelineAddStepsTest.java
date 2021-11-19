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
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.InjectModelCatalog;
import org.neo4j.gds.core.ModelCatalogExtension;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.model.catalog.TestTrainConfig;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.DEFAULT_PARAM_CONFIG;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationSplitConfig.DEFAULT_CONFIG;

@ModelCatalogExtension
class NodeClassificationPipelineAddStepsTest extends BaseProcTest {

    @InjectModelCatalog
    ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() {
        NodeClassificationPipelineCreate.create(modelCatalog, getUsername(), "myPipeline");
    }

    @AfterEach
    void tearDown() {
        modelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldAddNodePropertyStep() {
        run(caller -> {
            var result = NodeClassificationPipelineAddSteps.addNodeProperty(
                getUsername(),
                caller,
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
            assertThat(result.featureSteps).isEqualTo(List.of());
            assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
        });
    }

    @Test
    void shouldAddFeatureSteps() {
        run(caller -> {
            NodeClassificationPipelineAddSteps.addFeatures(
                getUsername(),
                "myPipeline",
                "test"
            );
            var result = NodeClassificationPipelineAddSteps.addFeatures(
                getUsername(),
                "myPipeline",
                List.of("pr", "pr2")
            );
            assertThat(result.name).isEqualTo("myPipeline");
            assertThat(result.splitConfig).isEqualTo(DEFAULT_CONFIG.toMap());
            assertThat(result.nodePropertySteps).isEqualTo(List.of());
            assertThat(result.featureSteps).isEqualTo(List.of("test", "pr", "pr2"));
            assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
        });
    }

    @Test
    void failOnIncompleteNodePropertyStepConfig() {
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "myPipeline",
            "fastRP",
            Map.of()
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Multiple errors in configuration arguments:")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `embeddingDimension`")
            .hasMessageContaining("No value specified for the mandatory configuration parameter `mutateProperty`"));
    }

    @Test
    void failOnDuplicateMutateProperty() {
        run(caller -> {
            NodeClassificationPipelineAddSteps.addNodeProperty(
                getUsername(),
                caller,
                "myPipeline",
                "pageRank",
                Map.of("mutateProperty", "pr")
            );
            assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
                getUsername(),
                caller,
                "myPipeline",
                "pageRank",
                Map.of("mutateProperty", "pr")
            ))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "The value of `mutateProperty` is expected to be unique, but pr was already specified in the mutate procedure.");
        });
    }

    @Test
    void failOnUnexpectedConfigKeysInNodePropertyStepConfig() {
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "myPipeline",
            "pageRank",
            Map.of("mutateProperty", "pr", "destroyEverything", true)
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unexpected configuration key: destroyEverything"));
    }

    @Test
    void shouldAddNodeAndFeatureSteps() {
        run(caller -> {
            NodeClassificationPipelineAddSteps.addNodeProperty(
                getUsername(),
                caller,
                "myPipeline",
                "pageRank",
                Map.of("mutateProperty", "pr")
            );
            NodeClassificationPipelineAddSteps.addFeatures(
                getUsername(),
                "myPipeline",
                "pr"
            );
            var result = NodeClassificationPipelineAddSteps.addFeatures(
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
            assertThat(result.featureSteps).isEqualTo(List.of("pr", "pr2"));
            assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
        });
    }

    @Test
    void shouldAddTwoNodePropertySteps() {
        run(caller -> {
                NodeClassificationPipelineAddSteps.addNodeProperty(
                    getUsername(),
                    caller,
                    "myPipeline",
                    "pageRank",
                    Map.of("mutateProperty", "pr")
                );
            var result = NodeClassificationPipelineAddSteps.addNodeProperty(
                getUsername(),
                caller,
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
                ))
            );
            assertThat(result.featureSteps).isEqualTo(List.of());
            assertThat(result.parameterSpace).isEqualTo(DEFAULT_PARAM_CONFIG);
        });
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForNodePropertyStep() {
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "ceci n'est pas une pipe",
            "pageRank",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Model with name `ceci n'est pas une pipe` does not exist."));
    }

    @Test
    void shouldThrowIfPipelineDoesntExistForFeatureStep() {
        assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addFeatures(
            getUsername(),
            "ceci n'est pas une pipe",
            "test"
        ))
            .isExactlyInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Model with name `ceci n'est pas une pipe` does not exist.");
    }

    @Test
    void shouldThrowInvalidNodePropertyStepName() {
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "myPipeline",
            "juggleSpoons",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid procedure name `juggleSpoons` for pipelining."));
    }

    @Test
    void failOnConfiguringReservedConfigFields() {
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "myPipeline",
            "pageRank",
            Map.of("nodeLabels", List.of("LABEL"), "mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "Cannot configure ['nodeLabels', 'relationshipTypes'] for an individual node property step, but can only be configured at `train` and `predict` mode."
            ));
    }

    @Test
    void shouldThrowIfAddingNodePropertyToANonPipeline() {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GraphSchema.empty(),
            "testData",
            TestTrainConfig.of(),
            Map::of
        );

        modelCatalog.set(model1);
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addNodeProperty(
            getUsername(),
            caller,
            "testModel1",
            "pageRank",
            Map.of("mutateProperty", "pr")
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "Steps can only be added to a model of type `Node classification training pipeline`. But model `testModel1` is of type `testAlgo1`."
            ));
    }

    @Test
    void shouldThrowIfAddingFeatureToANonPipeline() {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GraphSchema.empty(),
            "testData",
            TestTrainConfig.of(),
            Map::of
        );

        modelCatalog.set(model1);
        run(caller -> assertThatThrownBy(() -> NodeClassificationPipelineAddSteps.addFeatures(
            getUsername(),
            "testModel1",
            "something"
        ))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "Steps can only be added to a model of type `Node classification training pipeline`. But model `testModel1` is of type `testAlgo1`."
            ));
    }

    private void run(Consumer<BaseProc> procConsumer) {
        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, procConsumer::accept);
    }
}
