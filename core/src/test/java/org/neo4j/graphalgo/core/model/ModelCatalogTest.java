/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.TrainConfig;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModelCatalogTest {

    private static final String USERNAME = "testUser";

    @BeforeEach
    void setUp() {
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @AfterEach
    void afterEach() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void shouldStoreModels() {
        Model<String, TestTrainConfig> model = Model.of("user1", "testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );
        Model<String, TestTrainConfig> model2 = Model.of("user2", "testModel2", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);
        ModelCatalog.set(model2);

        assertEquals(model, ModelCatalog.get("user1", "testModel", String.class, TestTrainConfig.class));
        assertEquals(model2, ModelCatalog.get("user2", "testModel2", String.class, TestTrainConfig.class));
    }

    @Test
    void shouldThrowWhenTryingToGetOtherUsersModel() {
        Model<String, TestTrainConfig> model = Model.of("user1", "testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get("fakeUser", "testModel", String.class, TestTrainConfig.class)
        );

        assertEquals("No model with model name `testModel` was found.", ex.getMessage());
        assertNotNull(model.creationTime());
    }

    @Test
    void onlyAllowOneCatalogInCE() {
        GdsEdition.instance().setToCommunityEdition();

        Model<String, TestTrainConfig> model = Model.of(USERNAME, "testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);


        Model<Long, TestTrainConfig> model2 = Model.of(USERNAME,"testModel2", "testAlgo", 1337L,
            TestTrainConfig.of()
        );

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.set(model2)
        );

        assertEquals("Community users can only store one model in the catalog", ex.getMessage());
    }


    @Test
    void shouldThrowOnMissingModel() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get(USERNAME, "something", String.class, TestTrainConfig.class)
        );

        assertEquals("No model with model name `something` was found.", ex.getMessage());
    }

    @Test
    void shouldThrowOnModelDataTypeMismatch() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME, "testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get(USERNAME, "testModel", Double.class, TestTrainConfig.class)
        );

        assertEquals(
            "The model `testModel` has data with different types than expected. " +
            "Expected data type: `java.lang.String`, invoked with model data type: `java.lang.Double`.",
            ex.getMessage()
        );
    }

    @Test
    void shouldThrowOnModelConfigTypeMismatch() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME, "testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get(USERNAME, "testModel", String.class, ModelCatalogTestTrainConfig.class)
        );

        assertEquals(
            "The model `testModel` has a training config with different types than expected. " +
            "Expected train config type: `org.neo4j.graphalgo.model.catalog.TestTrainConfigImpl`, " +
            "invoked with model config type: `org.neo4j.graphalgo.core.model.ModelCatalogTest$ModelCatalogTestTrainConfig`.",
            ex.getMessage()
        );
    }

    @Test
    void checksIfModelExists() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME, "testModel", "testAlgo", "modelData", TestTrainConfig.of());

        ModelCatalog.set(model);

        assertTrue(ModelCatalog.exists(USERNAME, "testModel"));
        assertFalse(ModelCatalog.exists(USERNAME, "bogusModel"));
        assertFalse(ModelCatalog.exists("fakeUser", "testModel"));
    }

    @Test
    void getModelType() {
        Model<String, TestTrainConfig> model = Model.of(
            USERNAME,
            "testModel",
            "testAlgo",
            "testData",
            TestTrainConfig.of()
        );
        ModelCatalog.set(model);

        Optional<String> testModel = ModelCatalog.type(USERNAME, "testModel");
        assertFalse(testModel.isEmpty());
        assertEquals("testAlgo", testModel.get());
    }

    @Test
    void getNonExistingModelType() {
        Optional<String> bogusModel = ModelCatalog.type(USERNAME, "bogusModel");
        assertTrue(bogusModel.isEmpty());
    }

    @Test
    void shouldDropModel() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME,  "testModel", "testAlgo", "modelData", TestTrainConfig.of());
        ModelCatalog.set(model);

        assertTrue(ModelCatalog.exists(USERNAME, "testModel"));
        ModelCatalog.drop(USERNAME, "testModel");
        assertFalse(ModelCatalog.exists(USERNAME, "testModel"));
    }

    @Test
    void cantDropOtherUsersModel() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME,  "testModel", "testAlgo", "modelData", TestTrainConfig.of());
        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.drop("fakeUser", "testModel")
        );

        assertEquals("Model with name `testModel` does not exist and can't be removed.", ex.getMessage());
    }

    @Test
    void failsWhenTryingToDropNonExistingModel() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.drop(USERNAME, "something")
        );

        assertEquals("Model with name `something` does not exist and can't be removed.", ex.getMessage());
    }

    @Test
    void shouldListModels() {
        Model<String, TestTrainConfig> model1 = Model.of(USERNAME, "testModel1", "testAlgo1", "modelData1", TestTrainConfig.of());
        Model<Long, TestTrainConfig> model2 = Model.of(USERNAME, "testModel2", "testAlgo2", 1337L, TestTrainConfig.of());
        ModelCatalog.set(model1);
        ModelCatalog.set(model2);


        Collection<Model<?, ?>> models = ModelCatalog.list(USERNAME);
        assertEquals(2, models.size());
        Map<String, ? extends Model<?, ?>> modelsMap = models
            .stream()
            .collect(Collectors.toMap(Model::name, model -> model));

        assertEquals(model1, modelsMap.get(model1.name()));
        assertEquals(model2, modelsMap.get(model2.name()));
    }

    @Test
    void shouldReturnEmptyList() {
        assertEquals(0, ModelCatalog.list(USERNAME).size());
    }

    @Test
    void shouldThrowOnOverridingModels() {
        Model<String, TestTrainConfig> model = Model.of(USERNAME, "testModel", "testAlgo", "modelData", TestTrainConfig.of());
        ModelCatalog.set(model);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.set(model)
        );

        assertEquals("Model with name `testModel` already exists", ex.getMessage());
    }

    @ValueClass
    @Configuration("ModelCatalogTestTrainConfigImpl")
    @SuppressWarnings("immutables:subtype")
    interface ModelCatalogTestTrainConfig extends BaseConfig, TrainConfig {

        static ModelCatalogTestTrainConfig of() {
            return ImmutableModelCatalogTestTrainConfig.of("username", "modelName");
        }
    }
}
