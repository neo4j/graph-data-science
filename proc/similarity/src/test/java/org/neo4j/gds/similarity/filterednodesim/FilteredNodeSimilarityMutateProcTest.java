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
package org.neo4j.gds.similarity.filterednodesim;

import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgoBaseProcTest;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.MemoryEstimateTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.gds.TestSupport.assertGraphEquals;

class FilteredNodeSimilarityMutateProcTest extends BaseProcTest implements
    AlgoBaseProcTest<NodeSimilarity, FilteredNodeSimilarityMutateConfig, NodeSimilarityResult>,
    MemoryEstimateTest<NodeSimilarity, FilteredNodeSimilarityMutateConfig, NodeSimilarityResult> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person)" +
        ", (b:Person)" +
        ", (c:Person)" +
        ", (d:Person)" +
        ", (i1:Item)" +
        ", (i2:Item)" +
        ", (i3:Item)" +
        ", (i4:Item)" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)" +
        ", (d)-[:LIKES]->(i1)" +
        ", (d)-[:LIKES]->(i2)" +
        ", (d)-[:LIKES]->(i3)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(GraphProjectProc.class, getProcedureClazz());
    }

    @Override
    public Class<? extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig, ?>> getProcedureClazz() {
        return FilteredNodeSimilarityMutateProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public FilteredNodeSimilarityMutateConfig createConfig(CypherMapWrapper userInput) {
        return FilteredNodeSimilarityMutateConfig.of(userInput);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", "score");
        }
        if (!mapWrapper.containsKey("mutateRelationshipType")) {
            mapWrapper = mapWrapper.withString("mutateRelationshipType", "SIMILAR_TO");
        }
        return mapWrapper;
    }

    @Override
    public void assertResultEquals(NodeSimilarityResult result1, NodeSimilarityResult result2) {
        assertGraphEquals(result1.graphResult().similarityGraph(), result2.graphResult().similarityGraph());
    }
}
