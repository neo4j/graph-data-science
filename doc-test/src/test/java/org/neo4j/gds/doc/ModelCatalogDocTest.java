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
package org.neo4j.gds.doc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.model.ModelConfig;

import java.util.Map;
import java.util.Optional;

@Neo4jModelCatalogExtension
abstract class ModelCatalogDocTest extends SingleFileDocTestBase {

    @Inject
    ModelCatalog modelCatalog;

    @BeforeEach
    void loadModel() {
        var exampleModel1 = Model.of(
            "example-model-type",
            GraphSchema.empty(),
            new Object(),
            new ExampleTrainConfig("my-model1"),
            new ExampleCustomInfo(Map.of("exampleModelInfo", "exampleValue"))
        );
        var exampleModel2 = Model.of(
            "example-model-type",
            GraphSchema.empty(),
            new Object(),
            new ExampleTrainConfig("my-model2"),
            new ExampleCustomInfo(Map.of("number", 42L))
        );
        modelCatalog.set(exampleModel1);
        modelCatalog.set(exampleModel2);
    }

    @AfterEach
    void afterAll() {
        modelCatalog.removeAllLoadedModels();
    }

    private static final class ExampleCustomInfo implements Model.CustomInfo {

        private final Map<String, Object> customInfo;

        private ExampleCustomInfo(Map<String, Object> customInfo) {this.customInfo = customInfo;}

        @Override
        public Map<String, Object> toMap() {
            return customInfo;
        }
    }

    private static final class ExampleTrainConfig implements BaseConfig, ModelConfig {

        private final String modelName;

        ExampleTrainConfig(String modelName) {
            this.modelName = modelName;
        }

        @Override
        public Optional<String> usernameOverride() {
            return Optional.empty();
        }

        @Override
        public String modelName() {
            return modelName;
        }

        @Override
        public String modelUser() {
            return "";
        }
    }
}
