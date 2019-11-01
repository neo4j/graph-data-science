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
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class MemRecProcTest extends ProcTestBase {

    @BeforeEach
    void setUp() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
                MemRecProc.class,
                PageRankProc.class,
                UnionFindProc.class,
                LabelPropagationProc.class,
                GraphLoadProc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void memrecProcedure() {
        String availableProcedures = "the available and supported procedures are {graph.load, labelPropagation, pageRank, unionFind}.";
        test(
                "algo.memrec(null, null, null, {})",
                "Missing procedure parameter, " + availableProcedures);
        test(
                "algo.memrec(null, null, 'doesNotExist', {direction: 'BOTH', graph: 'huge'})",
                "The procedure [doesNotExist] does not support memrec or does not exist, " + availableProcedures);

        test("algo.memrec(null, null, 'pageRank', {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");
        test("algo.pageRank.memrec(null, null, { graph: 'huge'})");

        test("algo.memrec(null, null, 'graph.load')");
        test("algo.graph.load.memrec(null, null)");

        test("algo.memrec(null, null, 'labelPropagation')");
        test("algo.memrec(null, null, 'labelPropagation', {direction: 'BOTH', graph: 'huge'})");
        test("algo.labelPropagation.memrec(null, null)");
        test("algo.labelPropagation.memrec(null, null, {direction: 'BOTH', graph: 'huge'})");
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
