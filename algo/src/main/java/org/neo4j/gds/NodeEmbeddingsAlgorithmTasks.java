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

import fastrp.FastRPParameters;
import hashgnn.HashGNNParameters;
import org.neo4j.gds.api.Graph;
import node2vec.Node2VecParameters;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.embeddings.fastrp.FastRPTask;
import org.neo4j.gds.embeddings.hashgnn.HashGNNTask;
import org.neo4j.gds.embeddings.node2vec.Node2VecTask;

import java.util.List;

public final class NodeEmbeddingsAlgorithmTasks {

    public Task fastRP(Graph graph, FastRPParameters fastRPParameters){
        return FastRPTask.create(graph.nodeCount(),graph.relationshipCount(),fastRPParameters);
    }

    public Task node2Vec(Graph graph, Node2VecParameters parameters){
        return Node2VecTask.create(graph,parameters);
    }

    public Task hashGNN(Graph graph, HashGNNParameters parameters, List<String> relationshipTypes){
        return HashGNNTask.create(graph, parameters, relationshipTypes);
    }
}
