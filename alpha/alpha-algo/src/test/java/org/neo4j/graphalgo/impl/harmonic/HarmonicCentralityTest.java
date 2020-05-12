/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.harmonic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HarmonicCentralityTest extends AlgoTestBase {

    public static final String DB_CYPHER =
        "CREATE (a:Node {name:'a'})" +
        ",      (b:Node {name:'b'})" +
        ",      (c:Node {name:'c'})" +
        ",      (d:Node {name:'d'})" +
        ",      (e:Node {name:'e'})" +
        ",      (a)-[:TYPE]->(b)" +
        ",      (b)-[:TYPE]->(c)" +
        ",      (d)-[:TYPE]->(e)";

    @BeforeEach
    void loadGraph() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldComputeHarmonicCentrality() {
        var graph = new StoreLoaderBuilder()
            .api(db)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();

        var harmonicCentrality = new HarmonicCentrality(
            graph,
            AllocationTracker.EMPTY,
            1,
            Pools.DEFAULT
        );

        harmonicCentrality.compute();

        assertEquals(0.375, harmonicCentrality.getCentralityScore(0), 0.1);
        assertEquals(0.5, harmonicCentrality.getCentralityScore(1), 0.1);
        assertEquals(0.375, harmonicCentrality.getCentralityScore(2), 0.1);
        assertEquals(0.25, harmonicCentrality.getCentralityScore(3), 0.1);
        assertEquals(0.25, harmonicCentrality.getCentralityScore(4), 0.1);
    }
}