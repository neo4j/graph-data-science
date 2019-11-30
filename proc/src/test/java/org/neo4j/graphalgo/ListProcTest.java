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
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphalgo.wcc.WccProc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ListProcTest extends ProcTestBase {

    private static final List<String> PROCEDURES = asList(
        "algo.beta.graph.generate",
        "algo.beta.k1coloring",
        "algo.beta.k1coloring.memrec",
        "algo.beta.k1coloring.stream",
        "algo.beta.labelPropagation",
        "algo.beta.labelPropagation.stream",
        "algo.beta.louvain",
        "algo.beta.louvain.stream",
        "algo.beta.wcc",
        "algo.beta.wcc.memrec",
        "algo.beta.wcc.pregel",
        "algo.beta.wcc.pregel.stream",
        "algo.beta.wcc.stream",
        "algo.graph.info",
        "algo.graph.list",
        "algo.graph.load",
        "algo.graph.load.memrec",
        "algo.graph.remove",
        "algo.labelPropagation",
        "algo.labelPropagation.memrec",
        "algo.labelPropagation.stream",
        "algo.louvain",
        "algo.louvain.memrec",
        "algo.louvain.stream",
        "algo.memrec",
        "algo.pageRank",
        "algo.pageRank.memrec",
        "algo.pageRank.stream",
        "algo.unionFind",
        "algo.unionFind.forkJoin",
        "algo.unionFind.forkJoin.stream",
        "algo.unionFind.forkJoinMerge",
        "algo.unionFind.forkJoinMerge.stream",
        "algo.unionFind.memrec",
        "algo.unionFind.queue",
        "algo.unionFind.queue.stream",
        "algo.unionFind.stream",
        "algo.wcc",
        "algo.wcc.memrec",
        "algo.wcc.stream"
    );

    private static final List<String> FUNCTIONS = Arrays.asList(
        "algo.asNode",
        "algo.asNodes",
        "algo.getNodeById",
        "algo.getNodesById",
        "algo.version"
    );

    private static final List<String> PAGE_RANK = Arrays.asList(
        "algo.pageRank",
        "algo.pageRank.memrec",
        "algo.pageRank.stream"
    );

    private static final List<String> ALL = Stream.concat(PROCEDURES.stream(), FUNCTIONS.stream()).collect(Collectors.toList());

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphLoadProc.class,
            GraphGenerateProc.class,
            K1ColoringProc.class,
            LabelPropagationProc.class,
            ListProc.class,
            LouvainProc.class,
            MemRecProc.class,
            PageRankProc.class,
            UnionFindProc.class,
            WccProc.class
        );
        registerFunctions(
            GetNodeFunc.class,
            VersionFunc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldListAllThingsExceptTheListProcedure() {
        assertEquals(ALL, listProcs(null));
    }

    @Test
    void listFilteredResult() {
        assertEquals(PAGE_RANK, listProcs("pageRank"));
        assertEquals(singletonList("algo.pageRank.stream"), listProcs("pageRank.stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    @Test
    void listFunctions() {
        List<String> actual = listProcs("asNode");
        actual.addAll(listProcs("getNode"));
        actual.addAll(listProcs("version"));
        assertEquals(FUNCTIONS, actual);
    }

    @Test
    void listEmpty() {
        String query = "CALL algo.list()";
        assertEquals(
            ALL,
            db.execute(query)
                .<String>columnAs("name")
                .stream()
                .collect(Collectors.toList())
        );
    }

    private List<String> listProcs(Object name) {
        String query = "CALL algo.list($name)";
        return db.execute(query, MapUtil.map("name", name))
            .<String>columnAs("name")
            .stream()
            .collect(Collectors.toList());
    }
}
