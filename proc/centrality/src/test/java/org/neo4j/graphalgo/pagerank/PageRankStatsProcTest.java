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
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PageRankStatsProcTest extends PageRankProcTest<PageRankStatsConfig> {

    private static final String DB_CYPHER = "CREATE " +
                                            "  (a:Label1 {name: 'a'})" +
                                            ", (b:Label1 {name: 'b'})" +
                                            ", (a)-[:REL]->(b)";

    @BeforeEach
    void setupGraph() throws Exception {
        super.setupGraph();
        runQuery(db, "MATCH (n) DETACH DELETE n", Collections.emptyMap());
        runQuery(DB_CYPHER);
    }

    @Override
    public Class<? extends AlgoBaseProc<PageRank, PageRank, PageRankStatsConfig>> getProcedureClazz() {
        return PageRankStatsProc.class;
    }

    @Override
    public PageRankStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankStatsConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("pageRank")
            .statsMode()
            .yields(
                "createMillis",
                "computeMillis",
                "didConverge",
                "ranIterations",
                "centralityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));

                assertEquals(true, row.get("didConverge"));
                assertEquals(2L, row.get("ranIterations"));

                assertNotNull(row.get("centralityDistribution"));
            }
        );
    }
}
