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
package org.neo4j.gds.embeddings.graphsage.batch;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.BatchProvider;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class BatchProviderTest {

    @GdlGraph
    private static final String GRAPH =
        "(a)-[]->(b)-[]->(c), " +
        "(a)-[]->(d), " +
        "(a)-[]->(e), " +
        "(a)-[]->(f), " +
        "(h), " +
        "(g)-[]->(i), " +
        "(i)-[]->(a), " +
        "(i)-[]->(j), " +
        "(j)-[]->(b)";

    @Inject
    private Graph graph;

    @Test
    void testBatching() {

        BatchProvider provider = new BatchProvider(3);
        List<long[]> layerNodes = provider.stream(graph).collect(toList());

        assertEquals(4, layerNodes.size());
    }

}
