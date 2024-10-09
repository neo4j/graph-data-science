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
package org.neo4j.gds.pricesteiner;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.LongToDoubleFunction;

public class PCSTFast extends Algorithm<PrizeSteinerTreeResult> {

    private final Graph graph;
    private final LongToDoubleFunction prizes; //figure out how to expose to user

    public PCSTFast(Graph graph, LongToDoubleFunction prizes, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.prizes = prizes;
    }

    @Override
    public PrizeSteinerTreeResult compute() {
        progressTracker.beginSubTask("PrizeCollectingSteinerTree");
        var growthResult = growthPhase();
        var treeStructure = TreeProducer.createTree(growthResult, graph.nodeCount(),graph.rootIdMap(),progressTracker);

        var strongPruning = new StrongPruning(treeStructure,growthResult.activeOriginalNodes(),prizes,progressTracker,terminationFlag);
        strongPruning.performPruning();

        progressTracker.endSubTask("PrizeCollectingSteinerTree");
        return strongPruning.resultTree();

    }

    private GrowthResult growthPhase(){
        var growthPhase =  new GrowthPhase(graph,prizes, progressTracker,terminationFlag);
        return growthPhase.grow();
    }


}
