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
package org.neo4j.gds.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Stream;

public class SimilarityGraphResult {
    private final Graph similarityGraph;
    private final long comparedNodes;
    private final boolean isTopKGraph;

    public SimilarityGraphResult(Graph similarityGraph, long comparedNodes, boolean isTopKGraph) {
        this.similarityGraph = similarityGraph;
        this.comparedNodes = comparedNodes;
        this.isTopKGraph = isTopKGraph;
    }

    public Graph similarityGraph() {
        return similarityGraph;
    }

    public Direction direction() {
        return similarityGraph
            .schema()
            .direction();
    }

    public long comparedNodes() {
        return comparedNodes;
    }

    public boolean isTopKGraph() {
        return isTopKGraph;
    }


    public static SimilarityGraphResult empty(){
        return  new SimilarityGraphResult(null,0,false);
    }

    public static SimilarityGraphResult fromStream(
        IdMap idMap,
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream,
        TerminationFlag terminationFlag
    ) {
        var similarityGraph =  new SimilarityGraphBuilder(
            idMap,
            concurrency,
            DefaultPool.INSTANCE,
            terminationFlag
        ).build(similarityResultStream);

        return new SimilarityGraphResult(similarityGraph, idMap.nodeCount(), false);
    }

}
