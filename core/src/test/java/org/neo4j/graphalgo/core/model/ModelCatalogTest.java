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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.BaseConfig;
import org.neo4j.graphalgo.config.TrainConfig;
import org.neo4j.graphalgo.model.catalog.TestTrainConfig;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModelCatalogTest {

    @Test
    void shouldStoreModels() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        assertEquals(model, ModelCatalog.get("testModel", String.class, TestTrainConfig.class));
    }

    @Test
    void shouldThrowOnMissingModel() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get("something", String.class, TestTrainConfig.class)
        );

        assertEquals("No model with model name `something` was found.", ex.getMessage());
    }

    @Test
    void shouldThrowOnModelDataTypeMismatch() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get("testModel", Double.class, TestTrainConfig.class)
        );

        assertEquals(
            "The model `testModel` has data with different types than expected. " +
            "Expected data type: `java.lang.String`, invoked with model data type: `java.lang.Double`.",
            ex.getMessage()
        );
    }

    @Test
    void shouldThrowOnModelConfigTypeMismatch() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "testTrainData",
            TestTrainConfig.of()
        );

        ModelCatalog.set(model);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get("testModel", String.class, ModelCatalogTestTrainConfig.class)
        );

        assertEquals(
            "The model `testModel` has a training config with different types than expected. " +
            "Expected train config type: `org.neo4j.graphalgo.model.catalog.ImmutableTestTrainConfig`, " +
            "invoked with model config type: `org.neo4j.graphalgo.core.model.ModelCatalogTest$ModelCatalogTestTrainConfig`.",
            ex.getMessage()
        );
    }


    @Test
    void checksIfModelExists() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "modelData", TestTrainConfig.of());

        ModelCatalog.set(model);

        assertTrue(ModelCatalog.exists("testModel"));
        assertFalse(ModelCatalog.exists("bogusModel"));
    }

    @Test
    void getModelType() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "testData", TestTrainConfig.of());
        ModelCatalog.set(model);

        Optional<String> testModel = ModelCatalog.type("testModel");
        assertFalse(testModel.isEmpty());
        assertEquals("testAlgo", testModel.get());
    }

    @Test
    void getNonExistingModelType() {
        Optional<String> bogusModel = ModelCatalog.type("bogusModel");
        assertTrue(bogusModel.isEmpty());
    }

    @Test
    void shouldDropModel() {
        Model<String, TestTrainConfig> model = Model.of("testModel", "testAlgo", "modelData", TestTrainConfig.of());
        ModelCatalog.set(model);

        assertTrue(ModelCatalog.exists("testModel"));
        ModelCatalog.drop("testModel");
        assertFalse(ModelCatalog.exists("testModel"));
    }

    @Test
    void failsWhenTryingToDropNonExistingModel() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.drop("something")
        );

        assertEquals("Model with name `something` does not exist and can't be removed.", ex.getMessage());
    }

    @Test
    void shouldListModels() {
        Model<String, TestTrainConfig> model1 = Model.of("testModel1", "testAlgo1", "modelData1", TestTrainConfig.of());
        Model<Long, TestTrainConfig> model2 = Model.of("testModel2", "testAlgo2", 1337L, TestTrainConfig.of());
        ModelCatalog.set(model1);
        ModelCatalog.set(model2);


        Collection<Model<?, ?>> models = ModelCatalog.list();
        assertEquals(2, models.size());
        Map<String, ? extends Model<?, ?>> modelsMap = models
            .stream()
            .collect(Collectors.toMap(Model::name, model -> model));

        assertEquals(model1, modelsMap.get(model1.name()));
        assertEquals(model2, modelsMap.get(model2.name()));
    }

    @Test
    void shouldReturnEmptyList() {
        assertEquals(0, ModelCatalog.list().size());
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
