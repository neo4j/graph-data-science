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
package org.neo4j.graphalgo.algo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.MemRecProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class MemRecProcTest {

    private static GraphDatabaseAPI db;

    @BeforeAll
    static void setUp() throws Exception {
        db = LdbcDownloader.openDb("Yelp");
        Procedures procedures = db
                .getDependencyResolver()
                .resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.FIRST);
        procedures.registerProcedure(MemRecProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(GraphLoadProc.class);
    }

    @AfterAll
    static void tearDown() {
        db.shutdown();
    }

    @Test
    void memrecProcedure() {
        test(
                "algo.memrec(null, null, null, {direction: 'BOTH', graph: 'huge'})",
                "Missing procedure parameter, the available and supported procedures are {graph.load, pageRank, unionFind}.");
        test(
                "algo.memrec(null, null, 'doesNotExist', {direction: 'BOTH', graph: 'huge'})",
                "The procedure [doesNotExist] does not support memrec or does not exist, the available and supported procedures are {graph.load, pageRank, unionFind}.");

        test("algo.memrec(null, null, 'pageRank', {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, { graph: 'huge'})");
        test("algo.memrec(null, null, 'graph.load')");
        test("algo.graph.load.memrec(null, null)");
    }

    private void test(final String s, final String expectedMessage) {
        test(s, Optional.ofNullable(expectedMessage));
    }

    private void test(final String s) {
        test(s, Optional.empty());
    }

    private void test(final String s, final Optional<String> expectedMessage) {
        String queryTemplate = "CALL %s YIELD nodes, relationships, requiredMemory, bytesMin, bytesMax RETURN nodes, relationships, requiredMemory, bytesMin, bytesMax";
        String query = String.format(queryTemplate, s);

        try {
            db.execute(query).resultAsString();
            expectedMessage.ifPresent(value -> fail("Call should have failed with " + value));
        } catch (QueryExecutionException e) {
            if (expectedMessage.isPresent()) {
                assertEquals(expectedMessage.get(), ExceptionUtil.rootCause(e).getMessage());
            } else {
                fail(e.getMessage());
            }
        }
    }

}
