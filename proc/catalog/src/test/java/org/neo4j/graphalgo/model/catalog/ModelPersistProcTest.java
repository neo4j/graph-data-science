/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.model.PersistedModel;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.ModelPersistenceSettings;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class ModelPersistProcTest extends ModelProcBaseTest {

    @TempDir
    Path tempDir;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(ModelPersistenceSettings.model_persistence_location, tempDir);
    }

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelPersistProc.class);
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void persistAModel() {
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

        assertCypherResult(
            "CALL gds.alpha.model.persist('testModel1')",
            List.of(
                map(
                    "modelName", modelName,
                    "persistMillis", greaterThan(0L)
                )
            )
        );

        assertThat(ModelCatalog.getUnsafe(getUsername(), modelName))
            .isInstanceOf(PersistedModel.class)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("persisted", true)
            .hasFieldOrPropertyWithValue("loaded", true);
    }

    @Test
    void persistAPersistedModel() {
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

        var query = "CALL gds.alpha.model.persist('testModel1')";

        runQuery(query);
        var persistedModel = ModelCatalog.getUnsafe(getUsername(), modelName);
        runQuery(query);

        assertThat(ModelCatalog.getUnsafe(getUsername(), modelName))
            .isEqualTo(persistedModel)
            .hasFieldOrPropertyWithValue("name", modelName)
            .hasFieldOrPropertyWithValue("persisted", true)
            .hasFieldOrPropertyWithValue("loaded", true);;
    }

}

