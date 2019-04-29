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
package org.neo4j.graphalgo.core.neo4jview;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.graphalgo.SimpleGraphSetup;
import org.neo4j.graphalgo.SimpleGraphTestCase;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknobloch
 */
//@Ignore("weights faulty")
public class GraphViewTest extends SimpleGraphTestCase {

    private static SimpleGraphSetup setup = new SimpleGraphSetup();

    @BeforeClass
    public static void setupGraph() {
        GraphSetup graphSetup = new GraphSetup(LABEL, RELATION, WEIGHT_PROPERTY, 0.0, null);
        graph = new GraphViewFactory((GraphDatabaseAPI) setup.getDb(), graphSetup).build();
        v0 = 0;
        v1 = 1;
        v2 = 2;
    }

    @AfterClass
    public static void tearDown() {
        if (setup != null) setup.shutdown();
        if (db != null) db.shutdown();
    }
}
