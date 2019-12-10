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

package org.neo4j.graphalgo.org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarity;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityConfigBase;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityFactory;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

import java.util.Optional;

public abstract class NodeSimilarityProcBase <CONFIG extends NodeSimilarityConfigBase> extends BaseAlgoProc<NodeSimilarity, NodeSimilarityResult, CONFIG> {

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    @Override
    protected AlgorithmFactory<NodeSimilarity, CONFIG> algorithmFactory(CONFIG config) {
        // TODO: Should check if we are writing or streaming, but how to do that in memrec?
        boolean computesSimilarityGraph = true;
        return new NodeSimilarityFactory<>(
            new NodeSimilarity.Config(
                config.similarityCutoff(),
                config.degreeCutoff(),
                config.normalizedN(),
                config.normalizedK(),
                config.concurrency(),
                ParallelUtil.DEFAULT_BATCH_SIZE,
                config.direction(),
                false
            ),
            computesSimilarityGraph
        );
    }
}
