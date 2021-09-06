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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.core.ModelStoreSettings;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.gds.compat.MapUtil.map;

@GdsEditionTest(Edition.EE)
class ModelStoreProcTest extends ModelProcBaseTest {

    @TempDir
    Path tempDir;

    TestLog testLog;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = new TestLog();
        builder.setUserLogProvider(new LogProvider() {
            @Override
            public Log getLog(Class<?> loggingClass) {
                return testLog;
            }

            @Override
            public Log getLog(String name) {
                return testLog;
            }
        });
        builder.setConfig(ModelStoreSettings.model_store_location, tempDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelStoreProc.class);
    }

    @Test
    void storeAModel() {
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

        ModelCatalog.set(model1);

        assertCypherResult(
            "CALL gds.alpha.model.store('testModel1')",
            List.of(
                map(
                    "modelName", modelName,
                    "storeMillis", greaterThanOrEqualTo(0L)
                )
            )
        );

        assertThat(ModelCatalog.getUntyped(getUsername(), modelName))
            .isInstanceOf(StoredModel.class)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("stored", true)
            .hasFieldOrPropertyWithValue("loaded", true);
    }

    @Test
    void storeAStoredModel() {
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

        ModelCatalog.set(model1);

        var query = "CALL gds.alpha.model.store('testModel1')";

        runQuery(query);
        var storedModel = ModelCatalog.getUntyped(getUsername(), modelName);
        runQuery(query);

        assertThat(ModelCatalog.getUntyped(getUsername(), modelName))
            .isEqualTo(storedModel)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("stored", true)
            .hasFieldOrPropertyWithValue("loaded", true);
    }

    @Test
    void shouldLogAndReturnEmptyResultOnUnsupportedModel() {
        var modelName = "testModel1";
        var model1 = Model.of(
            getUsername(),
            modelName,
            "Link prediction pipeline",
            GRAPH_SCHEMA,
            "bogusModel",
            TestTrainConfig.of(),
            Map::of
        );

        ModelCatalog.set(model1);

        assertCypherResult("CALL gds.alpha.model.store('testModel1')", List.of());

        assertThat(testLog.getMessages(TestLog.DEBUG))
            .contains("Storing models of type `Link prediction pipeline` is not supported yet.");
    }

    @Test
    void doNotAllowToStoreModelsOnCE() {
        GdsEdition.instance().setToCommunityEdition();

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

        ModelCatalog.set(model1);

        var query = "CALL gds.alpha.model.store('testModel1')";

        assertThatThrownBy(() -> runQuery(query))
            .getRootCause()
            .hasMessageContaining("Storing a model")
            .hasMessageContaining("Neo4j Graph Data Science library Enterprise Edition.");
    }

}
