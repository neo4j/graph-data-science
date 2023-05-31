/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.similarity.nodesim;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
 class UnionGraphWeightedNodeSimilarityTest  {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
            "  (a1: A)," +
            "  (a2: A)," +
            "  (b1: B)," +
            "  (b2: B)," +
            "  (b3: B)," +
            "(a1)-[:R1 {w: 5}]->(b1),"+
            "(a1)-[:R2 {w: 10}]->(b2),"+
            "(a2)-[:R2 {w: 10}]->(b2),"+
            "(a2)-[:R1 {w: 8}]->(b3)";

    @Inject
    private Graph graph;

    @Test
    void shouldWorkWithUnionGraph(){

        var config = NodeSimilarityStreamConfigImpl.builder()
            .relationshipWeightProperty("w")
            .concurrency(1)
            .topN(1)
            .build();
        
        var nodeSimilarity = NodeSimilarity.create(graph, config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        var result = nodeSimilarity.compute().streamResult().findFirst().get();

        //input should be  (0 + 10 + 0)/ (5 + 10 + 8) = 10/23
        assertThat(result.similarity).isCloseTo((10) / (23.0), Offset.offset(1E-5));
    }
}
