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
package org.neo4j.graphalgo.model.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;

import static org.neo4j.graphalgo.compat.MapUtil.map;

class ModelListProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ModelListProc.class);
    }

    @Test
    void listsModel() {
        Model<String, TestTrainConfig> model1 = Model.of("testModel1", "testAlgo1", "testData", TestTrainConfig.of());
        Model<Long, TestTrainConfig> model2 = Model.of("testModel2", "testAlgo2", 1337L, TestTrainConfig.of());
        ModelCatalog.set(model1);
        ModelCatalog.set(model2);

        assertCypherResult(
            "CALL gds.beta.model.list()",
            List.of(
                map("modelInfo", map("modelName", "testModel1", "modelType", "testAlgo1")),
                map("modelInfo", map("modelName", "testModel2", "modelType", "testAlgo2"))
            )
        );
    }

    @Test
    void emptyListOnEmptyCatalog() {
        assertCypherResult(
            "CALL gds.beta.model.list()",
            List.of()
        );
    }
}
