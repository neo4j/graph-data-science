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
package org.neo4j.graphalgo.core.huge;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;

/**
 * @author mknobloch
 */
public class HugeGraphTest extends SimpleGraphTestCase {

    private static SimpleGraphSetup setup;

    @BeforeClass
    public static void setupGraph() {
        setup = new SimpleGraphSetup();
        graph = setup.build(HugeGraphFactory.class);
        v0 = setup.getV0();
        v1 = setup.getV1();
        v2 = setup.getV2();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (setup != null) setup.shutdown();
        if (db != null) db.shutdown();
    }
}
