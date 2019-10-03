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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphalgo.wcc.WccProc;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class EmptyGraphProcTest {

    private static GraphDatabaseAPI db;

    @BeforeAll
    public static void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        procedures.registerProcedure(LouvainProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(WccProc.class);
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.shutdown();
    }

    public String graphImpl = "huge";

    @Test
    public void testUnionFindStream() {
        Result result = db.execute("CALL algo.unionFind.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFind() throws Exception {
        db.execute("CALL algo.unionFind('', '',{graph:'" + graphImpl + "'}) YIELD nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testWCCStream() {
        Result result = db.execute("CALL algo.beta.wcc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testWCC() throws Exception {
        db.execute("CALL algo.beta.wcc('', '',{graph:'" + graphImpl + "'}) YIELD nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testLabelPropagationStream() {
        Result result = db.execute("CALL algo.labelPropagation.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    @Deprecated
    public void testLabelDeprecatedPropagation() throws Exception {
        db.execute("CALL algo.labelPropagation('', '', '', {graph:'" + graphImpl + "', writeProperty:'community'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testLabelBetaPropagation() throws Exception {
        db.execute("CALL algo.beta.labelPropagation('', '', {graph:'" + graphImpl + "', writeProperty:'community'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testLouvainStream() {
        Result result = db.execute("CALL algo.louvain.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testLouvain() throws Exception {
        db.execute("CALL algo.louvain('', '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testPageRankStream() {
        Result result = db.execute("CALL algo.pageRank.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testPageRank() throws Exception {
        db.execute("CALL algo.pageRank('', '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }
}
