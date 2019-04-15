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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.HarmonicCentralityProc;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class HarmonicCentralityIntegrationTest_477 {

    public static final String TYPE = "TYPE";

    @ClassRule
    public static final ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        db.execute(
                "CREATE (alice:Person{id:\"Alice\"}),\n" +
                        "       (michael:Person{id:\"Michael\"}),\n" +
                        "       (karin:Person{id:\"Karin\"}),\n" +
                        "       (chris:Person{id:\"Chris\"}),\n" +
                        "       (will:Person{id:\"Will\"}),\n" +
                        "       (mark:Person{id:\"Mark\"})\n" +
                        "CREATE (michael)-[:KNOWS]->(karin),\n" +
                        "       (michael)-[:KNOWS]->(chris),\n" +
                        "       (will)-[:KNOWS]->(michael),\n" +
                        "       (mark)-[:KNOWS]->(michael),\n" +
                        "       (mark)-[:KNOWS]->(will),\n" +
                        "       (alice)-[:KNOWS]->(michael),\n" +
                        "       (will)-[:KNOWS]->(chris),\n" +
                        "       (chris)-[:KNOWS]->(karin);"
        );

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(HarmonicCentralityProc.class);
    }

    @Test
    public void testLoad() throws Exception {

        String cypher = "CALL algo.closeness.harmonic.stream(\n" +
                "'MATCH (u:Person) RETURN id(u) as id\n" +
                "','\n" +
                "MATCH (u1:Person)-[k:KNOWS]-(u2:Person) \n" +
                "RETURN id(u1) as source,id(u2) as target\n" +
                "',{graph:'cypher'}) YIELD nodeId,centrality";


        db.execute(cypher);
    }
}
