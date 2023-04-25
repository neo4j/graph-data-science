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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.model.catalog.ModelDropProc;
import org.neo4j.gds.model.catalog.ModelExistsProc;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;

class GraphSageIntegrationTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:King{ name: 'A', age: 20, birth_year: 200, death_year: 300 })" +
        ", (b:King{ name: 'B', age: 12, birth_year: 232, death_year: 300 })" +
        ", (c:King{ name: 'C', age: 67, birth_year: 212, death_year: 300 })" +
        ", (d:King{ name: 'D', age: 78, birth_year: 245, death_year: 300 })" +
        ", (e:King{ name: 'E', age: 32, birth_year: 256, death_year: 300 })" +
        ", (f:King{ name: 'F', age: 32, birth_year: 214, death_year: 300 })" +
        ", (g:King{ name: 'G', age: 35, birth_year: 214, death_year: 300 })" +
        ", (h:King{ name: 'H', age: 56, birth_year: 253, death_year: 300 })" +
        ", (i:King{ name: 'I', age: 62, birth_year: 267, death_year: 300 })" +
        ", (j:King{ name: 'J', age: 44, birth_year: 289, death_year: 300 })" +
        ", (k:King{ name: 'K', age: 89, birth_year: 211, death_year: 300 })" +
        ", (l:King{ name: 'L', age: 99, birth_year: 201, death_year: 300 })" +
        ", (m:King{ name: 'M', age: 99, birth_year: 201, death_year: 300 })" +
        ", (n:King{ name: 'N', age: 99, birth_year: 201, death_year: 300 })" +
        ", (o:King{ name: 'O', age: 99, birth_year: 201, death_year: 300 })" +
        ", (a)-[:REL {weight: 1.0}]->(b)" +
        ", (a)-[:REL {weight: 5.0}]->(c)" +
        ", (b)-[:REL {weight: 42.0}]->(c)" +
        ", (b)-[:REL {weight: 10.0}]->(d)" +
        ", (c)-[:REL {weight: 62.0}]->(e)" +
        ", (d)-[:REL {weight: 1.0}]->(e)" +
        ", (d)-[:REL {weight: 1.0}]->(f)" +
        ", (e)-[:REL {weight: 1.0}]->(f)" +
        ", (e)-[:REL {weight: 4.0}]->(g)" +
        ", (h)-[:REL {weight: 1.0}]->(i)" +
        ", (i)-[:REL {weight: -1.0}]->(j)" +
        ", (j)-[:REL {weight: 1.0}]->(k)" +
        ", (j)-[:REL {weight: -10.0}]->(l)" +
        ", (k)-[:REL {weight: 1.0}]->(l)";

    private static final String graphName = "embeddingsGraph";

    private static final String modelName = "graphSageModel";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphSageStreamProc.class,
            GraphSageWriteProc.class,
            GraphSageMutateProc.class,
            GraphSageTrainProc.class,
            ModelExistsProc.class,
            ModelDropProc.class
        );

        String query = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("King")
            .withNodeProperty(PropertyMapping.of("age", 1.0))
            .withNodeProperty(PropertyMapping.of("birth_year", 1.0))
            .withNodeProperty(PropertyMapping.of("death_year", 1.0))
            .withRelationshipType(
                "R",
                RelationshipProjection.of(
                    "*",
                    Orientation.UNDIRECTED
                )
            )
            .withRelationshipProperty("weight")
            .yields();

        runQuery(query);
    }
    private static final int EMBEDDING_SIZE = 42;

    @Test
    void shouldRun() {
        modelDoesntExist();

        train();

        modelExists();

        stream();

        dropModel();

        modelDoesntExist();
    }

    private void modelDoesntExist() {
        checkModelExistence("n/a", false);
    }

    private void train() {
        String trainQuery = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("sampleSizes", List.of(2, 4))
            .addParameter("featureProperties", List.of("age", "birth_year", "death_year"))
            .addParameter("embeddingDimension", GraphSageIntegrationTest.EMBEDDING_SIZE)
            .addParameter("activationFunction", ActivationFunction.SIGMOID)
            .addParameter("aggregator", "mean")
            .addParameter("modelName", modelName)
            .yields();

        runQuery(trainQuery);
    }


    private void modelExists() {
        checkModelExistence(GraphSage.MODEL_TYPE, true);
    }

    private void checkModelExistence(String modelType, boolean exists) {
        assertCypherResult(
            "CALL gds.beta.model.exists($modelName)",
            Map.of("modelName", modelName),
            List.of(
                Map.of(
                    "modelName", modelName,
                    "modelType", modelType,
                    "exists", exists
                )
            )
        );
    }

    private void stream() {
        String streamQuery = GdsCypher.call("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("batchSize", 5)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithRowConsumer(streamQuery, row -> {
            assertThat(row.getNumber("nodeId"))
                .isNotNull();

            assertThat(row.get("embedding"))
                .asList()
                .hasSize(GraphSageIntegrationTest.EMBEDDING_SIZE);
        });
    }

    private void dropModel() {
        assertCypherResult(
            "CALL gds.beta.model.drop($modelName)",
            Map.of("modelName", modelName),
            singletonList(
                Map.of(
                    "modelInfo", allOf(
                        hasEntry("modelName", modelName),
                        hasEntry("modelType", GraphSage.MODEL_TYPE),
                        hasEntry(equalTo("metrics"), isA(Map.class))
                    ),
                    "creationTime", isA(ZonedDateTime.class),
                    "trainConfig", allOf(
                        aMapWithSize(21),
                        hasEntry("modelName", modelName),
                        hasEntry("aggregator", "MEAN"),
                        hasEntry("activationFunction", "SIGMOID")
                    ),
                    "loaded", true,
                    "stored", false,
                    "graphSchema", isA(Map.class),
                    "shared", false
                )
            )
        );
    }
}
