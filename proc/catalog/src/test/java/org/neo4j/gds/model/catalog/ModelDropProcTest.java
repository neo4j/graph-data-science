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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.core.ModelStoreSettings;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ModelDropProcTest extends ModelProcBaseTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelDropProc.class);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void dropsModel() {
        String existingModel = "testModel";
        String testModelType = "testAlgo";

        TestTrainConfig trainConfig = TestTrainConfig.of();
        ModelCatalog.set(
            Model.of(
                getUsername(),
                existingModel,
                testModelType,
                GRAPH_SCHEMA,
                "testData",
                trainConfig,
                Map::of
            )
        );

        var dropQuery = "CALL gds.beta.model.drop($modelName)";
        assertCypherResult(
            dropQuery,
            Map.of("modelName", existingModel),
            singletonList(
                map(
                    "modelInfo", map("modelName", existingModel, "modelType", testModelType),
                    "trainConfig", map(
                        "dummyConfigProperty", trainConfig.dummyConfigProperty(),
                        "modelName", trainConfig.modelName(),
                        "sudo", trainConfig.sudo(),
                        "username", trainConfig.usernameOverride()
                    ),
                    "loaded", true,
                    "stored", false,
                    "graphSchema", EXPECTED_SCHEMA,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                )
            )
        );
    }

    @Test
    void failOnDroppingNonExistingModel() {
        String modelName = "foo";
        assertError(
            "CALL gds.beta.model.drop($modelName)",
            map("modelName", modelName),
            formatWithLocale("Model with name `%s` does not exist.", modelName)
        );
    }

    @Nested
    class ModelDropProcStoredModelsTest extends ModelProcBaseTest {
        @TempDir
        Path tempDir;

        @Override
        @ExtensionCallback
        protected void configuration(TestDatabaseManagementServiceBuilder builder) {
            super.configuration(builder);
            builder.setConfig(ModelStoreSettings.model_store_location, tempDir);
        }

        @Test
        void dropLoadedModel() throws IOException {
            var modelName = "testModel1";
            var model1 = Model.of(
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
                GraphSageModelTrainer.GraphSageTrainMetrics.empty()
            );
            ModelStoreProc.storeModel(db, model1);

            assertCypherResult(
                "CALL gds.beta.model.drop('testModel1') YIELD loaded, stored",
                singletonList(
                    map(
                        "loaded", false,
                        "stored", true
                    )
                )
            );
        }

        @Test
        void returnStoredButUnloadedModel() throws IOException {
            var modelName = "testModel1";
            var model1 = Model.of(
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
                GraphSageModelTrainer.GraphSageTrainMetrics.empty()
            );
            ModelStoreProc.storeModel(db, model1);
            ModelCatalog.getUntyped(getUsername(), modelName).unload();

            assertCypherResult(
                "CALL gds.beta.model.drop('testModel1') YIELD loaded, stored",
                singletonList(
                    map(
                        "loaded", false,
                        "stored", true
                    )
                )
            );
        }

    }
}
