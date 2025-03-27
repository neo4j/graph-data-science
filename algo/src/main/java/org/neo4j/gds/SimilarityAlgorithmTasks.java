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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.similarity.filteredknn.FilteredKNNTask;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnParameters;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityParameters;
import org.neo4j.gds.similarity.knn.KnnParameters;
import org.neo4j.gds.similarity.knn.KnnTask;
import org.neo4j.gds.similarity.nodesim.FilteredNodeSimilarityTask;

public final class SimilarityAlgorithmTasks {

    public Task filteredKnn(Graph graph, FilteredKnnParameters parameters){
        return FilteredKNNTask.create(graph.nodeCount(), parameters);
    }

    public Task knn(Graph graph, KnnParameters parameters){
        return KnnTask.create(graph.nodeCount(), parameters);
    }

    public Task filteredNodeSimilarity(Graph graph, FilteredNodeSimilarityParameters filteredNodeSimilarityParameters){
        return FilteredNodeSimilarityTask.create(graph,filteredNodeSimilarityParameters.nodeSimilarityParameters());
    }

}
