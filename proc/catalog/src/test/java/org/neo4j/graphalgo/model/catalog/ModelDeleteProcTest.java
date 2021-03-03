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
package org.neo4j.graphalgo.model.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.graphalgo.core.ModelStoreSettings;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTestCase;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.graphalgo.compat.MapUtil.map;

@GdsEditionTestCase(Edition.EE)
class ModelDeleteProcTest extends ModelProcBaseTest {

    @TempDir
    Path tempDir;

    public static final String MODEL_NAME = "testModel1";

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(ModelStoreSettings.model_store_location, tempDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelStoreProc.class, ModelLoadProc.class, ModelDeleteProc.class);

        storeModelInCatalog();
        runQuery("CALL gds.alpha.model.store($modelName)", Map.of("modelName", MODEL_NAME));
        ((StoredModel) ModelCatalog.getUntyped(getUsername(), MODEL_NAME)).unload();
    }

    private void storeModelInCatalog() {
        var model1 = Model.of(
            getUsername(),
            MODEL_NAME,
            GraphSage.MODEL_TYPE,
            GRAPH_SCHEMA,
            ModelData.of(new Layer[]{}, new SingleLabelFeatureFunction()),
            ImmutableGraphSageTrainConfig.builder()
                .username(getUsername())
                .modelName(MODEL_NAME)
                .degreeAsProperty(true)
                .build()
        );

        ModelCatalog.set(model1);
    }

    @Test
    void deleteAnUnloadedModel() {
        assertCypherResult(
            "CALL gds.alpha.model.delete($modelName)",
            Map.of("modelName", MODEL_NAME),
            List.of(
                map(
                    "modelName", MODEL_NAME,
                    "deleteMillis", greaterThanOrEqualTo(0L)
                )
            )
        );

        assertThat(ModelCatalog.exists(getUsername(), MODEL_NAME)).isFalse();
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void deleteALoadedModel() {
        var query = "CALL gds.alpha.model.load($modelName)";
        runQuery(query, Map.of("modelName", MODEL_NAME));

        assertCypherResult(
            "CALL gds.alpha.model.delete($modelName)",
            Map.of("modelName", MODEL_NAME),
            List.of(
                map(
                    "modelName", MODEL_NAME,
                    "deleteMillis", greaterThanOrEqualTo(0L)
                )
            )
        );

        assertThat(ModelCatalog.exists(getUsername(), MODEL_NAME)).isTrue();
        assertThat(ModelCatalog.getUntyped(getUsername(), MODEL_NAME))
            .isNotInstanceOf(StoredModel.class)
            .hasFieldOrPropertyWithValue("name", MODEL_NAME)
            .hasFieldOrPropertyWithValue("loaded", true);
        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void deleteANonExistingModel() {
        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery("CALL gds.alpha.model.delete('doesNotExist')"))
            .havingRootCause()
            .isInstanceOf(NoSuchElementException.class)
            .withMessage("Model with name `doesNotExist` does not exist.");
    }

    @Test
    void deleteANonStoredModel() {
        ModelCatalog.drop(getUsername(), MODEL_NAME);
        storeModelInCatalog();

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery("CALL gds.alpha.model.delete($modelName)", Map.of("modelName", MODEL_NAME)))
            .havingRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .withMessage("The model testModel1 is not stored.");
    }

}
