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

        assertEquals(model, ModelCatalog.get("testModel"));
    }

    @Test
    void shouldThrowOnMissingModel() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> ModelCatalog.get("something")
        );

        assertEquals("No model with model name `something` was found.", ex.getMessage());
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
}
