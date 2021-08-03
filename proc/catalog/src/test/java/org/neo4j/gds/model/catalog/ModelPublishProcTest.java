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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.core.ModelStoreSettings;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.compat.MapUtil.map;

@GdsEditionTest(Edition.EE)
class ModelPublishProcTest extends ModelProcBaseTest {

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
            ModelPublishProc.class,
            ModelListProc.class
        );
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldPublishModel() {
        var modelName = "testModel1";
        var model1 = Model.of(
            getUsername(),
            modelName,
            GraphSage.MODEL_TYPE,
            GRAPH_SCHEMA,
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            TestTrainConfig.of()
        );

        ModelCatalog.set(model1);

        // Make sure the model exists in the catalog
        assertExistingModel(modelName, false);

        // Publish the model
        assertCypherResult(
            "CALL gds.alpha.model.publish($modelName)",
            map("modelName", modelName),
            singletonList(
                map(
                    "modelInfo", map("modelName", modelName + "_public", "modelType", "graphSage"),
                    "trainConfig", map(
                        "dummyConfigProperty", TestTrainConfig.of().dummyConfigProperty(),
                        "modelName", TestTrainConfig.of().modelName(),
                        "sudo", TestTrainConfig.of().sudo(),
                        "username", TestTrainConfig.of().usernameOverride()
                    ),
                    "graphSchema", EXPECTED_SCHEMA,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", true,
                    "loaded", true,
                    "stored", false
                )
            )
        );

        // Check that there is no longer model with the name it had before publishing
        assertNonExistingModel(modelName);

        // Check that the published model can be listed back
        assertExistingModel("testModel1_public", true);
    }

    private void assertNonExistingModel(String modelName) {
        assertCypherResult(
            "CALL gds.beta.model.list($modelName)",
            map("modelName", modelName),
            List.of()
        );
    }

    private void assertExistingModel(String modelName, boolean shared) {
        assertCypherResult(
            "CALL gds.beta.model.list($modelName)",
            map("modelName", modelName),
            singletonList(
                map(
                    "modelInfo", map("modelName", modelName, "modelType", "graphSage"),
                    "trainConfig", map(
                        "dummyConfigProperty", TestTrainConfig.of().dummyConfigProperty(),
                        "modelName", TestTrainConfig.of().modelName(),
                        "sudo", TestTrainConfig.of().sudo(),
                        "username", TestTrainConfig.of().usernameOverride()
                    ),
                    "graphSchema", EXPECTED_SCHEMA,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", shared,
                    "loaded", true,
                    "stored", false
                )
            )
        );
    }
}
