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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public class KGEPredictAlgorithmFactory<CONFIG extends KGEPredictMutateConfig> extends GraphStoreAlgorithmFactory<TopKMapComputer, CONFIG> {

    @Override
    public TopKMapComputer build(
        GraphStore graphOrGraphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {

        BitSet sourceNodes = new BitSet(graphOrGraphStore.nodeCount());
        BitSet targetNodes = new BitSet(graphOrGraphStore.nodeCount());

        Graph graph = graphOrGraphStore.getGraph();

        graph.forEachNode(nodeId -> {
            if (graph.nodeLabels(nodeId).contains(NodeLabel.of(configuration.sourceNodeLabel()))) {
                sourceNodes.set(nodeId);
            }
            if (graph.nodeLabels(nodeId).contains(NodeLabel.of(configuration.targetNodeLabel()))) {
                targetNodes.set(nodeId);
            }
            return true;
        });


        return new TopKMapComputer(
            graph,
            sourceNodes,
            targetNodes,
            configuration.nodeEmbeddingProperty(),
            configuration.relationshipTypeEmbedding(),
            configuration.scoringFunction(),
            (s, t) -> s != t, //TODO s-t should not be an existing edge
            configuration.topK(),
            configuration.concurrency(),
            progressTracker
        );
    }

    @Override
    public String taskName() {
        return "KGEPredict";
    }

}
