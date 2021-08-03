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
package org.neo4j.gds.model.catalog;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.core.ModelStoreSettings;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdsEditionTest(Edition.EE)
class ModelStoreAndPublishIntegrationTest extends ModelProcBaseTest {

    @TempDir
    Path tempDir;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(ModelStoreSettings.model_store_location, tempDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            ModelStoreProc.class,
            ModelPublishProc.class,
            ModelDropProc.class,
            ModelDeleteProc.class
        );
    }

    @Test
    void storeAndPublishAModel() {
        var modelName = "testModel1";
        var publishedModelName = modelName + "_public";

        modelToCatalog(modelName);

        storeModel(modelName);

        modelShouldBeLoadedInCatalog(modelName);

        checkStoredModelFile(modelName);

        publishModel(modelName);

        modelShouldNotBeInCatalog(modelName);

        modelShouldBeLoadedInCatalog(publishedModelName);

        checkPublishedModelFile(modelName);

        dropStoredModel(publishedModelName);

        modelShouldBeUnloadedInCatalog(publishedModelName);

        deletedModel(publishedModelName);

        checkModelFileIsDeleted();
    }

    private void modelToCatalog(String modelName) {
        var model = Model.of(
            getUsername(),
            modelName,
            GraphSage.MODEL_TYPE,
            GRAPH_SCHEMA,
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig.builder()
                .username(getUsername())
                .modelName(modelName)
                .addFeatureProperties("a")
                .build(),
            EmptyGraphSageTrainMetrics.INSTANCE
        );

        ModelCatalog.set(model);

    }

    private void storeModel(String modelName) {
        assertCypherResult(
            "CALL gds.alpha.model.store($modelName)",
            map("modelName", modelName),
            List.of(
                map(
                    "modelName", modelName,
                    "storeMillis", greaterThanOrEqualTo(0L)
                )
            )
        );
    }

    private void modelShouldBeLoadedInCatalog(String modelName) {
        assertThat(ModelCatalog.getUntyped(getUsername(), modelName))
            .isInstanceOf(StoredModel.class)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("stored", true)
            .hasFieldOrPropertyWithValue("loaded", true);
    }

    private void modelShouldBeUnloadedInCatalog(String modelName) {
        assertThat(ModelCatalog.getUntyped(getUsername(), modelName))
            .isInstanceOf(StoredModel.class)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("stored", true)
            .hasFieldOrPropertyWithValue("loaded", false);
    }

    private void checkStoredModelFile(String modelName) {
        var modelFiles = tempDir.toFile().listFiles();
        assertThat(modelFiles).hasSize(1);
        var storedModelDir = modelFiles[0].toPath();

        // Check that the model is successfully stored on disk
        assertThatNoException().isThrownBy(() -> {
            var modelFromFile = new StoredModel(storedModelDir);
            modelFromFile.load();

            assertThat(modelFromFile)
                .isNotNull()
                .isInstanceOf(StoredModel.class)
                .hasFieldOrPropertyWithValue("name", modelName)
                .hasFieldOrPropertyWithValue("stored", true)
                .hasFieldOrPropertyWithValue("loaded", true);
        });
    }

    private void publishModel(String modelName) {
        assertCypherResult(
            "CALL gds.alpha.model.publish($modelName)",
            map("modelName", modelName),
            singletonList(
                map(
                    "modelInfo",
                    map("modelName",
                        modelName + "_public",
                        "modelType", "graphSage",
                        "metrics", Map.of(
                            "didConverge", false,
                            "ranEpochs", 0,
                            "epochLosses", List.of())
                    ),
                    "trainConfig", isA(Map.class),
                    "graphSchema", EXPECTED_SCHEMA,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", true,
                    "loaded", true,
                    "stored", true
                )
            )
        );
    }

    private void modelShouldNotBeInCatalog(String modelName) {
        assertThatThrownBy(() -> ModelCatalog.getUntyped(getUsername(), modelName))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessage(formatWithLocale("Model with name `%s` does not exist.", modelName));
    }

    private void checkPublishedModelFile(String modelName) {
        checkStoredModelFile(modelName + "_public");
    }

    private void dropStoredModel(String modelName) {
        dropModel(modelName, true);
    }

    private void dropModel(String modelName, boolean stored) {
        assertCypherResult(
            "CALL gds.beta.model.drop($modelName)",
            Map.of("modelName", modelName),
            singletonList(
                Map.of(
                    "modelInfo", map("modelName",
                        modelName,
                        "modelType", "graphSage",
                        "metrics", Map.of(
                            "didConverge", false,
                            "ranEpochs", 0,
                            "epochLosses", List.of())
                    ),
                    "creationTime", Matchers.isA(ZonedDateTime.class),
                    "trainConfig", Matchers.isA(Map.class),
                    "loaded", false,
                    "stored", stored,
                    "graphSchema", Matchers.isA(Map.class),
                    "shared", true
                )
            )
        );    }

    private void deletedModel(String modelName) {
        assertCypherResult(
            "CALL gds.alpha.model.delete($modelName)",
            Map.of("modelName", modelName),
            List.of(
                map(
                    "modelName", modelName,
                    "deleteMillis", greaterThanOrEqualTo(0L)
                )
            )
        );
    }

    private void checkModelFileIsDeleted() {
        assertThat(tempDir.toFile().listFiles()).isEmpty();
    }

}
