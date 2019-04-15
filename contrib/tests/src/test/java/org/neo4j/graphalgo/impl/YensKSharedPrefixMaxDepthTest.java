/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.KShortestPathsProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 * <pre>
 *         /-     (c)    -\
 * (a) - (b)               (d)
 *        \- (e) - (f) - /
 * </pre>
 */
public class YensKSharedPrefixMaxDepthTest {


    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:1.0}]->(b),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(c),\n" +
                        " (c)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (b)-[:TYPE {cost:1.0}]->(e),\n" +
                        " (e)-[:TYPE {cost:1.0}]->(f),\n" +
                        " (f)-[:TYPE {cost:1.0}]->(d)\n";

        db.execute(cypher);
        db.resolveDependency(Procedures.class).registerProcedure(KShortestPathsProc.class);
    }

    @Test
    public void testMaxDepthForKShortestPaths() {
        final String cypher =
                "MATCH (from:Node{name:{from}}), (to:Node{name:{to}}) " +
                        "CALL algo.kShortestPaths.stream(from, to, 2, 'cost', {path:true, maxDepth: {maxDepth}}) YIELD path " +
                        "RETURN path";

        Map<String, Object> params = new HashMap<>();
        params.put("from", "d");
        params.put("to", "a");
        params.put("maxDepth", 5);
        List<Object> paths = db.execute(cypher, params).stream().map(result -> result.get("path"))
                .collect(Collectors.toList());

        assertEquals("Number of paths to maxDepth=5 should be 1", 1, paths.size());

        // Other direction should work ok, right?
        params.put("from", "a");
        params.put("to", "d");

        List<Object> pathsOtherDirection = db.execute(cypher, params).stream().map(result -> result.get("path"))
                .collect(Collectors.toList());

        assertEquals("Number of paths to maxDepth=5 should be 1", 1, pathsOtherDirection.size());

        params.put("maxDepth", 6);

        List<Object> pathsDepth6 = db.execute(cypher, params).stream().map(result -> result.get("path"))
                .collect(Collectors.toList());

        assertEquals("Number of paths to maxDepth=6 should be 2", 2, pathsDepth6.size());
    }
}
