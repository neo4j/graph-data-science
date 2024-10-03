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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictBaseConfig;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictConfigTransformer;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictParameters;
import org.neo4j.gds.algorithms.machinelearning.TopKMapComputer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;

public class KGEPredictAlgorithmFactory<CONFIG extends KGEPredictBaseConfig> extends GraphStoreAlgorithmFactory<TopKMapComputer, CONFIG> {

    public TopKMapComputer build(
        GraphStore graphStore,
        KGEPredictParameters parameters,
        ProgressTracker progressTracker
    ) {
        BitSet sourceNodes = new BitSet(graphStore.nodeCount());
        BitSet targetNodes = new BitSet(graphStore.nodeCount());

        Graph graph = graphStore.getGraph(parameters.relationshipTypesFilter(), Optional.empty());

        var sourceNodeFilter = parameters.sourceNodeFilter().toNodeFilter(graph);
        var targetNodeFilter = parameters.targetNodeFilter().toNodeFilter(graph);

        graph.forEachNode(node -> {
            if (sourceNodeFilter.test(node)) {
                sourceNodes.set(node);
            }
            if (targetNodeFilter.test(node)) {
                targetNodes.set(node);
            }
            return true;
        });

        return new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            parameters.nodeEmbeddingProperty(),
            parameters.relationshipTypeEmbedding(),
            parameters.scoringFunction(),
            parameters.topK(),
            parameters.concurrency(),
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Override
    public TopKMapComputer build(
        GraphStore graphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return build(graphStore, KGEPredictConfigTransformer.toParameters(configuration), progressTracker);
    }

    @Override
    public String taskName() {
        return "KGEPredict";
    }

}
