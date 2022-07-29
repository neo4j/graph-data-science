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
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;

import java.util.Map;

import static java.util.Collections.singletonList;
import static org.neo4j.gds.compat.MapUtil.map;

@Neo4jModelCatalogExtension
class ModelExistsProcTest extends ModelProcBaseTest {

    private static final String EXISTS_QUERY = "CALL gds.beta.model.exists($modelName)";

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelExistsProc.class);
    }

    @Test
    void checksIfModelExists() {
        String existingModel = "testModel";
        modelCatalog.set(Model.of(
            "testAlgo",
            GRAPH_SCHEMA,
            "testData",
            TestTrainConfig.of(getUsername(), existingModel),
            Map::of
        ));

        assertCypherResult(
            EXISTS_QUERY,
            Map.of(
                "modelName", existingModel
            ),
            singletonList(
                map(
                    "modelName", existingModel,
                    "modelType", "testAlgo",
                    "exists", true
                )
            )
        );
    }

    @Test
    void returnsCorrectResultForNonExistingModel() {

        String bogusModel = "bogusModel";
        assertCypherResult(
            EXISTS_QUERY,
            Map.of(
                "modelName", bogusModel),
            singletonList(
                map(
                    "modelName", bogusModel,
                    "modelType", "n/a",
                    "exists", false
                )
            )
        );
    }
}
