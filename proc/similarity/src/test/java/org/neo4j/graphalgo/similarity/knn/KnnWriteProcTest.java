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
package org.neo4j.graphalgo.similarity.knn;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.ImmutablePropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.WriteRelationshipWithPropertyTest;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class KnnWriteProcTest extends KnnProcTest<KnnWriteConfig> implements WriteRelationshipWithPropertyTest<Knn, KnnWriteConfig, Knn.Result> {

    @Override
    public Class<? extends AlgoBaseProc<Knn, Knn.Result, KnnWriteConfig>> getProcedureClazz() {
        return KnnWriteProc.class;
    }

    @Override
    public KnnWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return KnnWriteConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        var map = super.createMinimalConfig(mapWrapper);
        if (!map.containsKey("writeProperty")) {
            map = map.withString("writeProperty", writeProperty());
        }
        if (!map.containsKey("writeRelationshipType")) {
            map = map.withString("writeRelationshipType", writeRelationshipType());
        }
        return map;
    }

    @Test
    void shouldWriteResults() {
        String query = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("gds", "beta", "knn")
            .writeMode()
            .addParameter("sudo", true)
            .addParameter("nodeWeightProperty", "knn")
            .addParameter("topK", 1)
            .addParameter("randomSeed", 42)
            .addParameter("writeRelationshipType", "SIMILAR")
            .addParameter("writeProperty", "score")
            .yields(
                "computeMillis",
                "createMillis",
                "nodesCompared ",
                "relationshipsWritten",
                "writeMillis",
                "similarityDistribution",
                "postProcessingMillis",
                "configuration"
            );

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3, row.getNumber("nodesCompared").longValue());
            assertEquals(3, row.getNumber("relationshipsWritten").longValue());
            assertUserInput(row, "writeRelationshipType", "SIMILAR");
            assertUserInput(row, "writeProperty", "score");
            assertThat("Missing computeMillis", -1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat("Missing createMillis", -1L, lessThan(row.getNumber("createMillis").longValue()));
            assertThat("Missing writeMillis", -1L, lessThan(row.getNumber("writeMillis").longValue()));

            Map<String, Double> distribution = (Map<String, Double>) row.get("similarityDistribution");
            assertThat("Missing min", -1.0, lessThan(distribution.get("min")));
            assertThat("Missing max", -1.0, lessThan(distribution.get("max")));
            assertThat("Missing mean", -1.0, lessThan(distribution.get("mean")));
            assertThat("Missing stdDev", -1.0, lessThan(distribution.get("stdDev")));
            assertThat("Missing p1", -1.0, lessThan(distribution.get("p1")));
            assertThat("Missing p5", -1.0, lessThan(distribution.get("p5")));
            assertThat("Missing p10", -1.0, lessThan(distribution.get("p10")));
            assertThat("Missing p25", -1.0, lessThan(distribution.get("p25")));
            assertThat("Missing p50", -1.0, lessThan(distribution.get("p50")));
            assertThat("Missing p75", -1.0, lessThan(distribution.get("p75")));
            assertThat("Missing p90", -1.0, lessThan(distribution.get("p90")));
            assertThat("Missing p95", -1.0, lessThan(distribution.get("p95")));
            assertThat("Missing p99", -1.0, lessThan(distribution.get("p99")));
            assertThat("Missing p100", -1.0, lessThan(distribution.get("p100")));

            assertThat(
                "Missing postProcessingMillis",
                -1L,
                equalTo(row.getNumber("postProcessingMillis").longValue())
            );
        });

        String resultGraphName = "simGraph";
        String loadQuery = GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("id")
            .withRelationshipType("SIMILAR")
            .withRelationshipProperty("score")
            .graphCreate(resultGraphName)
            .yields();

        runQuery(loadQuery);

        assertGraphEquals(
            fromGdl("(a {id: 1})-[{w: 0.5}]->(b {id: 2}), (b)-[{w: 0.5}]->(a), (c {id: 3})-[{w: 0.25}]->(b)"),
            GraphStoreCatalog.get(getUsername(), namedDatabaseId(), resultGraphName).graphStore().getUnion()
        );
    }

    @Override
    public String writeRelationshipType() {
        return "KNN_REL";
    }

    @Override
    public String writeProperty() {
        return "similarity";
    }

    @Override
    public void setupStoreLoader(StoreLoaderBuilder storeLoaderBuilder, Map<String, Object> config) {
        var nodeWeightProperty = config.get("nodeWeightProperty");
        if (nodeWeightProperty != null) {
            var nodeProperty = String.valueOf(nodeWeightProperty);
            runQuery(
                graphDb(),
                "CALL db.createProperty($prop)",
                Map.of("prop", nodeWeightProperty)
            );
            storeLoaderBuilder.addNodeProperty(
                ImmutablePropertyMapping.builder()
                    .propertyKey(nodeProperty)
                    .defaultValue(DefaultValue.forDouble())
                    .build()
            );
        }
    }
}
