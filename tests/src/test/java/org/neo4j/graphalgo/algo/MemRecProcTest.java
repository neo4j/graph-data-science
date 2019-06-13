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
import org.junit.Test;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.MemRecProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class MemRecProcTest {

    private GraphDatabaseAPI db;

    @Before
    public void setUp() throws Exception {
        db = LdbcDownloader.openDb("L01");
        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(MemRecProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(LouvainProc.class);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void memrecProcedure() {
        db.execute("CALL algo.memrec(null, null, 'pageRank', {direction:'BOTH', concurrency:4}) YIELD requiredMemory, treeView")
                .<String>columnAs("treeView")
                .forEachRemaining(System.out::println);
        db.execute("CALL algo.unionFind.memrec(null, null, {direction:'BOTH', concurrency:4}) YIELD requiredMemory, treeView")
                .<String>columnAs("treeView")
                .forEachRemaining(System.out::println);
    }

}
