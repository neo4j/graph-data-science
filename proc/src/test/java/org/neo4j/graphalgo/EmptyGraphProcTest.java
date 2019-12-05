/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphalgo.wcc.WccProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends ProcTestBase {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            LabelPropagationProc.class,
            LouvainWriteProc.class,
            LouvainStreamProc.class,
            PageRankProc.class,
            UnionFindProc.class,
            WccProc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    public String graphImpl = "huge";

    @Test
    public void testUnionFindStream() {
        Result result = runQuery("CALL algo.unionFind.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFind() throws Exception {
        runQuery(
            "CALL algo.unionFind('', '',{graph:'" + graphImpl + "'}) YIELD nodes",
            row -> assertEquals(0L, row.getNumber("nodes"))
        );
    }

    @Test
    public void testWCCStream() {
        Result result = runQuery("CALL algo.beta.wcc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testWCC() throws Exception {
        runQuery(
            "CALL algo.beta.wcc('', '',{graph:'" + graphImpl + "'}) YIELD nodes",
            row -> assertEquals(0L, row.getNumber("nodes"))
        );
    }

    @Test
    public void testLabelPropagationStream() {
        Result result = runQuery("CALL algo.labelPropagation.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    @Deprecated
    public void testLabelDeprecatedPropagation() throws Exception {
        runQuery(
            "CALL algo.labelPropagation('', '', '', {graph:'" + graphImpl + "', writeProperty:'community'})",
            row -> assertEquals(0L, row.getNumber("nodes"))
        );
    }

    @Test
    public void testLabelBetaPropagation() throws Exception {
        runQuery(
            "CALL algo.beta.labelPropagation('', '', {graph:'" + graphImpl + "', writeProperty:'community'})",
            row -> {
                assertEquals(0L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    public void testLouvainStream() {
        Result result = runQuery("CALL algo.louvain.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testLouvain() throws Exception {
        runQuery(
            "CALL algo.louvain('', '', {graph:'" + graphImpl + "'})",
            row -> assertEquals(0L, row.getNumber("nodes"))
        );
    }

    @Test
    public void testPageRankStream() {
        Result result = runQuery("CALL algo.pageRank.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testPageRank() throws Exception {
        runQuery(
            "CALL algo.pageRank('', '', {graph:'" + graphImpl + "'})",
            row -> assertEquals(0L, row.getNumber("nodes"))
        );
    }
}
