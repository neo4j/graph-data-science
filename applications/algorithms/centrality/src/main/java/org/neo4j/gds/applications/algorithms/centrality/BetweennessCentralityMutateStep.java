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
package org.neo4j.gds.applications.algorithms.centrality;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;

import java.util.Map;

class BetweennessCentralityMutateStep implements MutateOrWriteStep<BetwennessCentralityResult, Pair<Map<String, Object>, NodePropertiesWritten>> {
    private final MutateNodeProperty mutateNodeProperty;
    private final BetweennessCentralityMutateConfig configuration;
    private final boolean shouldComputeCentralityDistribution;

    BetweennessCentralityMutateStep(
        MutateNodeProperty mutateNodeProperty,
        BetweennessCentralityMutateConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        this.mutateNodeProperty = mutateNodeProperty;
        this.configuration = configuration;
        this.shouldComputeCentralityDistribution = shouldComputeCentralityDistribution;
    }

    @Override
    public Pair<Map<String, Object>, NodePropertiesWritten> execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        BetwennessCentralityResult result
    ) {
        return mutateNodeProperty.mutateNodeProperties(
            graph,
            graphStore,
            configuration,
            result.centralityScoreProvider(),
            shouldComputeCentralityDistribution,
            result.nodePropertyValues()
        );
    }
}
