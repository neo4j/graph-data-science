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

import org.hamcrest.Matchers;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainProc;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.gds.compat.MapUtil.map;

@GdsEditionTest(Edition.EE)
class GraphSageModelStoreAndPublishIntegrationTest extends BaseModelStoreAndPublishIntegrationTest {

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

    @Override
    Class<?> trainProcClass() {
        return GraphSageTrainProc.class;
    }

    @Override
    String dbQuery() {
        return DB_CYPHER;
    }

    @Override
    void createGraph() {
        String query = GdsCypher.call()
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
            .graphCreate("g")
            .yields();
        runQuery(query);
    }

    @Override
    protected void modelToCatalog(String modelName) {
        String train = GdsCypher.call().explicitCreation("g")
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("randomSeed", 19)
            .addParameter("featureProperties", List.of("age", "birth_year", "death_year"))
            .addParameter("aggregator", "mean")
            .addParameter("activationFunction", "sigmoid")
            .addParameter("embeddingDimension", 64)
            .addParameter("modelName", modelName)
            .yields();

        runQuery(train);
    }

    @Override
    protected void publishModel(String modelName) {
        assertCypherResult(
            "CALL gds.alpha.model.publish($modelName)",
            map("modelName", modelName),
            singletonList(
                map(
                    "modelInfo",
                    map("modelName",
                        modelName + "_public",
                        "modelType", "graphSage",
                        "metrics", Map.of(
                            "didConverge", false,
                            "ranEpochs", 1,
                            "epochLosses", List.of(398.6774315625296)
                        )
                    ),
                    "trainConfig", isA(Map.class),
                    "graphSchema", isA(Map.class),
                    "creationTime", isA(ZonedDateTime.class),
                    "shared", true,
                    "loaded", true,
                    "stored", true
                )
            )
        );
    }

    @Override
    protected void dropStoredModel(String modelName) {
        assertCypherResult(
            "CALL gds.beta.model.drop($modelName)",
            Map.of("modelName", modelName),
            singletonList(
                Map.of(
                    "modelInfo", map("modelName",
                        modelName,
                        "modelType", "graphSage",
                        "metrics", Map.of(
                            "didConverge", false,
                            "ranEpochs", 1,
                            "epochLosses", List.of(398.6774315625296)
                        )
                    ),
                    "creationTime", Matchers.isA(ZonedDateTime.class),
                    "trainConfig", Matchers.isA(Map.class),
                    "loaded", false,
                    "stored", true,
                    "graphSchema", Matchers.isA(Map.class),
                    "shared", true
                )
            )
        );
    }


}
