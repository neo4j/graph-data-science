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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.ModelStoreSettings;
import org.neo4j.gds.embeddings.graphsage.EmptyGraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ModelListProcTest extends ModelProcBaseTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelListProc.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.beta.model.list()", "gds.beta.model.list(null)"})
    void listsModel(String query) {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of()
        );

        var model2 = Model.of(
            getUsername(),
            "testModel2",
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of()
        );

        var otherUserModel = Model.of(
            "anotherUser",
            "testModel1337",
            "testAlgo1337",
            GRAPH_SCHEMA,
            3435L,
            TestTrainConfig.of()
        );

        ModelCatalog.set(model1);
        ModelCatalog.set(model2);
        ModelCatalog.set(otherUserModel);

        assertCypherResult(
            formatWithLocale(
                "CALL %s YIELD modelInfo, graphSchema, trainConfig, loaded, stored, creationTime, shared " +
                "RETURN modelInfo, graphSchema, trainConfig, loaded, stored, creationTime, shared " +
                "ORDER BY modelInfo.modelName",
                query
            ),
            List.of(
                map(
                    "modelInfo", map("modelName", "testModel1", "modelType", "testAlgo1"),
                    "graphSchema", EXPECTED_SCHEMA,
                    "trainConfig", TestTrainConfig.of().toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                ),
                map(
                    "modelInfo", map("modelName", "testModel2", "modelType", "testAlgo2"),
                    "graphSchema", EXPECTED_SCHEMA,
                    "trainConfig", TestTrainConfig.of().toMap(),
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                )
            )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CALL gds.beta.model.list()",
        "CALL gds.beta.model.list('someModel')"
    })
    void emptyResultOnListQueries(String query) {
        assertCypherResult(
            query,
            List.of()
        );
    }

    @Test
    void returnSpecificModel() {
        var model1 = Model.of(
            getUsername(),
            "testModel1",
            "testAlgo1",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of()
        );

        var model2 = Model.of(
            getUsername(),
            "testModel2",
            "testAlgo2",
            GRAPH_SCHEMA,
            1337L,
            TestTrainConfig.of()
        );

        ModelCatalog.set(model1);
        ModelCatalog.set(model2);

        assertCypherResult(
            "CALL gds.beta.model.list('testModel2')",
            singletonList(
                map(
                    "modelInfo", map("modelName", "testModel2", "modelType", "testAlgo2"),
                    "trainConfig", TestTrainConfig.of().toMap(),
                    "graphSchema", EXPECTED_SCHEMA,
                    "loaded", true,
                    "stored", false,
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", false
                )
            )
        );
    }

    @Nested
    class ModelListProcStoredModelsTest extends ModelProcBaseTest {
        @TempDir
        Path tempDir;

        @Override
        @ExtensionCallback
        protected void configuration(TestDatabaseManagementServiceBuilder builder) {
            super.configuration(builder);
            builder.setConfig(ModelStoreSettings.model_store_location, tempDir);
        }

        @Test
        void returnLoadedModel() throws IOException {
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
                EmptyGraphSageTrainMetrics.INSTANCE
            );
            ModelStoreProc.storeModel(db, model1);

            assertCypherResult(
                "CALL gds.beta.model.list('testModel1') YIELD loaded, stored",
                singletonList(
                    map(
                        "loaded", true,
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
                EmptyGraphSageTrainMetrics.INSTANCE
            );
            ModelStoreProc.storeModel(db, model1);
            ModelCatalog.getUntyped(getUsername(), modelName).unload();

            assertCypherResult(
                "CALL gds.beta.model.list('testModel1') YIELD loaded, stored",
                singletonList(
                    map(
                        "loaded", false,
                        "stored", true
                    )
                )
            );
        }

    }


    @ParameterizedTest(name = "`{0}`")
    @MethodSource("invalidModelNames")
    void failOnEmptyModelName(String modelName) {
        assertError(
            "CALL gds.beta.model.list($modelName)",
            map("modelName", modelName),
            "can not be null or blank"
        );
    }

    static Stream<String> invalidModelNames() {
        return Stream.of("", "   ", "           ", "\r\n\t");
    }
}
