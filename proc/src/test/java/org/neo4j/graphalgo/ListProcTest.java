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
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationWriteProc;
import org.neo4j.graphalgo.louvain.LouvainStreamProc;
import org.neo4j.graphalgo.louvain.LouvainWriteProc;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphalgo.pagerank.PageRankWriteProc;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ListProcTest extends ProcTestBase {

    private static final List<String> PROCEDURES = asList(
        "algo.beta.graph.generate",
        "algo.beta.k1coloring",
        "algo.beta.k1coloring.memrec",
        "algo.beta.k1coloring.stream",
        "algo.graph.info",
        "algo.graph.list",
        "algo.graph.load",
        "algo.graph.load.memrec",
        "algo.graph.remove",
        "algo.memrec",

        "gds.algo.labelPropagation.stats",
        "gds.algo.labelPropagation.stats.estimate",
        "gds.algo.labelPropagation.stream",
        "gds.algo.labelPropagation.stream.estimate",
        "gds.algo.labelPropagation.write",
        "gds.algo.labelPropagation.write.estimate",

        "gds.algo.louvain.stats",
        "gds.algo.louvain.stats.estimate",
        "gds.algo.louvain.stream",
        "gds.algo.louvain.stream.estimate",
        "gds.algo.louvain.write",
        "gds.algo.louvain.write.estimate",

        "gds.algo.pageRank.stats",
        "gds.algo.pageRank.stats.estimate",
        "gds.algo.pageRank.stream",
        "gds.algo.pageRank.stream.estimate",
        "gds.algo.pageRank.write",
        "gds.algo.pageRank.write.estimate",

        "gds.algo.wcc.stats",
        "gds.algo.wcc.stats.estimate",
        "gds.algo.wcc.stream",
        "gds.algo.wcc.stream.estimate",
        "gds.algo.wcc.write",
        "gds.algo.wcc.write.estimate"
        );

    private static final List<String> FUNCTIONS = asList(
        "algo.asNode",
        "algo.asNodes",
        "algo.getNodeById",
        "algo.getNodesById",
        "gds.version"
    );

    private static final List<String> PAGE_RANK = asList(
        "gds.algo.pageRank.stats",
        "gds.algo.pageRank.stats.estimate",
        "gds.algo.pageRank.stream",
        "gds.algo.pageRank.stream.estimate",
        "gds.algo.pageRank.write",
        "gds.algo.pageRank.write.estimate"
    );

    private static final List<String> ALL = Stream.concat(PROCEDURES.stream(), FUNCTIONS.stream()).collect(Collectors.toList());

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphLoadProc.class,
            GraphGenerateProc.class,
            K1ColoringProc.class,
            LabelPropagationWriteProc.class,
            LabelPropagationStreamProc.class,
            ListProc.class,
            LouvainWriteProc.class,
            LouvainStreamProc.class,
            MemRecProc.class,
            PageRankWriteProc.class,
            PageRankStreamProc.class,
            WccWriteProc.class,
            WccStreamProc.class
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
        assertEquals(asList("gds.algo.pageRank.stream", "gds.algo.pageRank.stream.estimate"), listProcs("pageRank.stream"));
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
        String query = "CALL gds.list()";
        assertEquals(
            ALL,
            runQuery(query)
                .<String>columnAs("name")
                .stream()
                .collect(Collectors.toList())
        );
    }

    private List<String> listProcs(Object name) {
        String query = "CALL gds.list($name)";
        return runQuery(query, MapUtil.map("name", name))
            .<String>columnAs("name")
            .stream()
            .collect(Collectors.toList());
    }
}
