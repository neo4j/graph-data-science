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
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.TestCustomInfo;
import org.neo4j.gds.model.catalog.TestTrainConfig;

import java.io.File;
import java.util.Map;

public abstract class ModelCatalogDocTestBase extends SingleFileDocTestBase {

    ModelCatalog modelCatalog;

    @BeforeEach
    @Override
    void setUp(@TempDir File workingDirectory) throws Exception {
        super.setUp(workingDirectory);
        this.modelCatalog = GraphDatabaseApiProxy.resolveDependency(defaultDb, ModelCatalog.class);

        var exampleModel1 = Model.of(
            "example-model-type",
            GraphSchema.empty(),
            new Object(),
            TestTrainConfig.of("", "my-model1"),
            new TestCustomInfo(Map.of("exampleModelInfo", "exampleValue"))
        );
        var exampleModel2 = Model.of(
            "example-model-type",
            GraphSchema.empty(),
            new Object(),
            TestTrainConfig.of("", "my-model2"),
            new TestCustomInfo(Map.of("number", 42L))
        );
        modelCatalog.set(exampleModel1);
        modelCatalog.set(exampleModel2);
    }

    @AfterEach
    void afterAll() {
        modelCatalog.removeAllLoadedModels();
    }
}
