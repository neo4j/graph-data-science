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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.pagerank.PageRank;
import org.neo4j.graphalgo.pagerank.PageRankStreamConfig;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeapControlTest extends BaseProcTest implements AlgoBaseProcTest<PageRankStreamConfig, PageRank> {
    static final String DB_CYPHER = "CREATE " +
                                    " (zhen:Person {name: 'Zhen'})," +
                                    " (praveena:Person {name: 'Praveena'})," +
                                    " (michael:Person {name: 'Michael'})," +
                                    " (arya:Person {name: 'Arya'})," +
                                    " (karin:Person {name: 'Karin'})," +

                                    " (zhen)-[:FRIENDS]->(arya)," +
                                    " (zhen)-[:FRIENDS]->(praveena)," +
                                    " (praveena)-[:WORKS_WITH]->(karin)," +
                                    " (praveena)-[:FRIENDS]->(michael)," +
                                    " (michael)-[:WORKS_WITH]->(karin)," +
                                    " (arya)-[:FRIENDS]->(karin)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(PageRankStreamProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldPassOnSufficientMemory () {
        applyOnProcedure(proc -> {
            MemoryTreeWithDimensions memoryTreeWithDimensions = proc.memoryEstimation(createConfig(CypherMapWrapper.empty()));
            proc.validateMemoryUsage(memoryTreeWithDimensions, () -> 10000000);
        });
    }

    @Test
    void shouldFailOnInsufficientMemory () {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            applyOnProcedure(proc -> {
                MemoryTreeWithDimensions memoryTreeWithDimensions = proc.memoryEstimation(createConfig(CypherMapWrapper.empty()));
                proc.validateMemoryUsage(memoryTreeWithDimensions, () -> 42);
            });
        });

        String message = ExceptionUtil.rootCause(exception).getMessage();
        assertTrue(message.matches(
            "Procedure was blocked since minimum estimated memory \\(\\d+\\) exceeds current free memory \\(42\\)."));
    }

    @Override
    public Class<? extends AlgoBaseProc<?, PageRank, PageRankStreamConfig>> getProcedureClazz() {
        return PageRankStreamProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public PageRankStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        CypherMapWrapper newMapWrapper = createMinimalImplicitConfig(CypherMapWrapper.empty());
        GraphCreateConfig implicitConfig = GraphCreateConfig.createImplicit("", newMapWrapper);
        return PageRankStreamConfig.of("", Optional.empty(), Optional.of(implicitConfig), newMapWrapper);
    }

    @Override
    public void assertResultEquals(PageRank result1, PageRank result2) {
    }
}
