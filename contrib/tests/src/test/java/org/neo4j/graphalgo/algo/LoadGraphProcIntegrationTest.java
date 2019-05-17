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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.LoadGraphProc;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.proc.LabelPropagationProc;
import org.neo4j.graphalgo.proc.PageRankProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(Parameterized.class)
public class LoadGraphProcIntegrationTest {

    private static final String DB_CYPHER = "" +
            "CREATE (a:A {id: 0, partition: 42}) " +
            "CREATE (b:B {id: 1, partition: 42}) " +

            "CREATE (a)-[:X]->(:A {id: 2, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 3, weight: 2.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 4, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 5, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 6, weight: 8.0, partition: 2}) " +

            "CREATE (b)-[:X]->(:B {id: 7, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 8, weight: 2.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 9, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 10, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 11, weight: 8.0, partition: 2})";

    @Parameterized.Parameters(name = "graph={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"heavy"},
                new Object[]{"huge"},
                new Object[]{"kernel"}
        );
    }

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private final String graph;

    public LoadGraphProcIntegrationTest(String graph) {
        this.graph = graph;
    }

    @Before
    public void setup() throws KernelException {
        Procedures procedures = db.resolveDependency(Procedures.class);
        procedures.registerProcedure(LoadGraphProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        db.execute(DB_CYPHER);
    }

    @After
    public void tearDown() throws Exception {
        LoadGraphFactory.remove("foo");
    }

    @Test
    public void shouldLoadGraph() {
        String query = "CALL algo.graph.load('foo',null,null,{graph:$graph})";

        runQuery(query, singletonMap("graph",graph), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(graph, row.getString("graph"));
            assertFalse(row.getBoolean("alreadyLoaded"));
        });
    }

    @Test
    public void shouldUseLoadedGraph() {
        db.execute("CALL algo.graph.load('foo',null,null,{graph:$graph})", singletonMap("graph",graph)).close();

        String query = "CALL algo.pageRank(null,null,{graph:$name,write:false})";
        runQuery(query, singletonMap("name","foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
    }

    @Test
    public void multiUseLoadedGraph() {
        db.execute("CALL algo.graph.load('foo',null,null,{graph:$graph})", singletonMap("graph",graph)).close();

        String query = "CALL algo.pageRank(null,null,{graph:$name,write:false})";
        runQuery(query, singletonMap("name","foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
        runQuery(query, singletonMap("name","foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
    }

    @Test
    public void shouldWorkWithLimitedTypes() {
        db.execute("CALL algo.graph.load('foo',null,null,{graph:$graph})", singletonMap("graph",graph)).close();

        String query = "CALL algo.labelPropagation(null,null,null,{graph:$name,write:false})";
        try {
            runQuery(query, singletonMap("name", "foo"), row -> {
                assertEquals(12, row.getNumber("nodes").intValue());
            });
        } catch (QueryExecutionException qee) {
            fail("Error using wrong graph type:" + qee.getMessage());
        }
    }

    @Test
    public void dontDoubleLoad() {
        String call = "CALL algo.graph.load('foo',null,null,{graph:$graph}) yield alreadyLoaded as loaded RETURN loaded";
        Map<String, Object> params = singletonMap("graph", this.graph);
        assertFalse(db.execute(call, params).<Boolean>columnAs("loaded").next());
        assertTrue(db.execute(call, params).<Boolean>columnAs("loaded").next());
    }

    @Test
    public void removeGraph() {
        db.execute("CALL algo.graph.load('foo',null,null,{graph:$graph})", singletonMap("graph",graph)).close();

        runQuery("CALL algo.graph.info($name)", singletonMap("name","foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("removed"));
            assertTrue(row.getBoolean("exists"));
        });
        runQuery("CALL algo.graph.remove($name)", singletonMap("name","foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQuery("CALL algo.graph.info($name)", singletonMap("name","foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    private void runQuery(String query, Map<String, Object> params, Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }
}
