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
package org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarity;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityFactory;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

import java.util.Optional;

public abstract class NodeSimilarityBaseProc<CONFIG extends NodeSimilarityBaseConfig> extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, CONFIG> {

    static final String NODE_SIMILARITY_DESCRIPTION =
        "The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. " +
        "Two nodes are considered similar if they share many of the same neighbors. " +
        "Node Similarity computes pair-wise similarities based on the Jaccard metric.";

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    @Override
    protected AlgorithmFactory<NodeSimilarity, CONFIG> algorithmFactory(CONFIG config) {
        return new NodeSimilarityFactory<>();
    }
}
