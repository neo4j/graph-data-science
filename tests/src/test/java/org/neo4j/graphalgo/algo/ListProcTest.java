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
import org.neo4j.graphalgo.ListProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author mh
 * @since 20.10.17
 */
class ListProcTest {

    private static GraphDatabaseAPI DB;

    private static final List<String> PROCEDURES = asList(
            "algo.pageRank",
            "algo.pageRank.memrec",
            "algo.pageRank.stream");

    private static final List<String> FUNCTIONS = Arrays.asList(
            "algo.linkprediction.adamicAdar",
            "algo.linkprediction.commonNeighbors",
            "algo.linkprediction.preferentialAttachment",
            "algo.linkprediction.resourceAllocation",
            "algo.linkprediction.sameCommunity",
            "algo.linkprediction.totalNeighbors");

    private static final List<String> ALL = Stream
            .of(PROCEDURES, FUNCTIONS)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    @BeforeAll
    static void setUp() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        Procedures procedures = DB.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(ListProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerFunction(LinkPredictionFunc.class);
    }

    @AfterAll
    static void tearDown() {
        DB.shutdown();
    }

    @Test
    void listProcedures() {
        assertEquals(ALL, listProcs(null));
        assertEquals(PROCEDURES, listProcs("page"));
        assertEquals(singletonList("algo.pageRank.stream"), listProcs("stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    @Test
    void listFunctions() {
        assertEquals(FUNCTIONS, listProcs("linkprediction"));
    }

    @Test
    void listEmpty() {
        assertEquals(ALL,
                DB.execute("CALL algo.list()").<String>columnAs("name").stream().collect(Collectors.toList()));
    }

    private List<String> listProcs(Object name) {
        return DB.execute("CALL algo.list($name)", singletonMap("name", name)).<String>columnAs("name").stream().collect(Collectors.toList());
    }
}
