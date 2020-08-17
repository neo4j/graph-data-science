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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.config.TrainConfig.MODEL_NAME_KEY;
import static org.neo4j.graphalgo.config.TrainConfig.MODEL_TYPE_KEY;

class GraphSageTrainProcTest extends GraphSageBaseProcTest {

    @Test
    void runsTraining() {
        String modelName = "gsModel";
        String graphName = "embeddingsGraph";
        String train = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.alpha.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("nodePropertyNames", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingSize", 64)
            .addParameter("degreeAsProperty", true)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithResultConsumer(train, result -> {
            Map<String, Object> resultRow = result.next();
            assertNotNull(resultRow);
            assertNotNull(resultRow.get("configuration"));
            assertEquals(graphName, resultRow.get("graphName"));
            Map<String, Object> modelInfo = (Map<String, Object>) resultRow.get("modelInfo");
            assertNotNull(modelInfo);
            assertEquals(modelName, modelInfo.get(MODEL_NAME_KEY));
            assertEquals(GraphSage.MODEL_TYPE, modelInfo.get(MODEL_TYPE_KEY));
            assertTrue((long) resultRow.get("trainMillis") > 0);
        });

        Model<Layer[]> model = (Model<Layer[]>) ModelCatalog.get(modelName);
        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());
    }

}
