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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class HashGNNMutateProcTest extends BaseProcTest implements AlgoBaseProcTest<HashGNN, HashGNNMutateConfig, HashGNN.HashGNNResult>, MutateNodePropertyTest<HashGNN, HashGNNMutateConfig, HashGNN.HashGNNResult> {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:N {f1: 1, f2: [0.0, 0.0]})" +
        ", (b:N {f1: 0, f2: [1.0, 0.0]})" +
        ", (c:N {f1: 0, f2: [0.0, 1.0]})" +
        ", (b)-[:R]->(a)" +
        ", (b)-[:R]->(c)";

    @BeforeEach
    void setupWritePropertiesProc() throws Exception {
        registerProcedures(GraphWriteNodePropertiesProc.class);
    }

    @Override
    public Class<? extends AlgoBaseProc<HashGNN, HashGNN.HashGNNResult, HashGNNMutateConfig, ?>> getProcedureClazz() {
        return HashGNNMutateProc.class;
    }

    @Override
    public GraphDatabaseService graphDb() {
        return db;
    }

    @Override
    public HashGNNMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return HashGNNMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        var minimalConfig = userInput
            .withNumber("iterations", 2)
            .withNumber("embeddingDensity", 2)
            .withNumber("randomSeed", 42L)
            .withEntry("featureProperties", List.of("f1", "f2"));

        if (!minimalConfig.containsKey("mutateProperty")) {
            return minimalConfig.withString("mutateProperty", "embedding");
        }
        return minimalConfig;
    }

    @Override
    public void assertResultEquals(HashGNN.HashGNNResult result1, HashGNN.HashGNNResult result2) {
        assertThat(result1.embeddings().size()).isEqualTo(result2.embeddings().size());
        for (int i = 0; i < result1.embeddings().size(); i++) {
            assertThat(result1.embeddings().get(i)).containsExactly(result2.embeddings().get(i));
        }
    }

    @Override
    public String mutateProperty() {
        return "embedding";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public List<String> nodeProperties() {
        return List.of("f1", "f2");
    }

    @Override
    public String expectedMutatedGraph() {
        return "CREATE" +
        "  (a {f1: 1, f2: [0.0, 0.0], embedding: [1.0, 0.0, 0.0]})" +
        ", (b {f1: 0, f2: [1.0, 0.0], embedding: [1.0, 0.0, 0.0]})" +
        ", (c {f1: 0, f2: [0.0, 1.0], embedding: [0.0, 0.0, 1.0]})" +
        ", (b)-[]->(a)" +
        ", (b)-[]->(c)";
    }
}
