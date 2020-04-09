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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PageRankMutateProcTest extends PageRankProcTest<PageRankMutateConfig> implements GraphMutationTest<PageRankMutateConfig, PageRank> {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1 {name: 'a'})" +
        ", (b:Label1 {name: 'b'})" +
        ", (c:Label1 {name: 'c'})" +
        ", (d:Label1 {name: 'd'})" +
        ", (e:Label1 {name: 'e'})" +
        ", (f:Label1 {name: 'f'})" +
        ", (g:Label1 {name: 'g'})" +
        ", (h:Label1 {name: 'h'})" +
        ", (i:Label1 {name: 'i'})" +
        ", (j:Label1 {name: 'j'})" +
        ", (b)-[:TYPE1]->(c)" +
        ", (c)-[:TYPE1]->(b)" +
        ", (d)-[:TYPE1]->(a)" +
        ", (d)-[:TYPE1]->(b)" +
        ", (e)-[:TYPE1]->(b)" +
        ", (e)-[:TYPE1]->(d)" +
        ", (e)-[:TYPE1]->(f)" +
        ", (f)-[:TYPE1]->(b)" +
        ", (f)-[:TYPE1]->(e)";

    @Override
    public String mutateProperty() {
        return "score";
    }

    @BeforeEach
    void setupGraph() throws Exception {
        super.setupGraph();
        runQuery(db, "MATCH (n) DETACH DELETE n", Collections.emptyMap());
        runQuery(DB_CYPHER);
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a {score: 0.243013d})" +
            ", (b {score: 1.847962d})" +
            ", (c {score: 1.712861d})" +
            ", (d {score: 0.218854d})" +
            ", (e {score: 0.243013d})" +
            ", (f {score: 0.218854d})" +
            ", (g {score: 0.150000d})" +
            ", (h {score: 0.150000d})" +
            ", (i {score: 0.150000d})" +
            ", (j {score: 0.150000d})" +
            ", (b)-[{w: 1.0d}]->(c)" +
            ", (c)-[{w: 1.0d}]->(b)" +
            ", (d)-[{w: 1.0d}]->(a)" +
            ", (d)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(b)" +
            ", (e)-[{w: 1.0d}]->(d)" +
            ", (e)-[{w: 1.0d}]->(f)" +
            ", (f)-[{w: 1.0d}]->(b)" +
            ", (f)-[{w: 1.0d}]->(e)";
    }

    @Override
    public Class<? extends AlgoBaseProc<?, PageRank, PageRankMutateConfig>> getProcedureClazz() {
        return PageRankMutateProc.class;
    }

    @Override
    public PageRankMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("pageRank")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields(
                "nodePropertiesWritten",
                "createMillis",
                "computeMillis",
                "mutateMillis",
                "didConverge",
                "ranIterations",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(10L, row.getNumber("nodePropertiesWritten"));

                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));

                assertEquals(false, row.get("didConverge"));
                assertEquals(20L, row.get("ranIterations"));
            }
        );
    }
}
