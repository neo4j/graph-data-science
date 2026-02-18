
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
package org.neo4j.gds.procedures.algorithms.similarity.stats;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityDistributionInstructions;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStatsResult;
import org.neo4j.gds.similarity.SimilarityComputeBusinessFacade;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsConfig;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackSimilarityStatsProcedureFacade {

    private final SimilarityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final SimilarityDistributionInstructions similarityDistributionInstructions;


    public PushbackSimilarityStatsProcedureFacade(
        SimilarityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        SimilarityDistributionInstructions similarityDistributionInstructions
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.similarityDistributionInstructions = similarityDistributionInstructions;
    }
    /*
    public Stream<SimilarityStreamResult> knn(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KnnStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.knn(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new KnnStreamResultTransformer(graphResources.graph())
        ).join();
    }

    public Stream<SimilarityStreamResult> filteredKnn(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, FilteredKnnStreamConfig::of);

        var parameters = config.toFilteredKnnParameters();
        return businessFacade.filteredKnn(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new FilteredKnnStreamResultTransformer(graphResources.graph())
        ).join();
    } */

    public Stream<SimilarityStatsResult> nodeSimilarity(
        String graphName,
        Map<String, Object> configuration

    ) {
        var config = configurationParser.parseConfiguration(configuration, NodeSimilarityStatsConfig::of);

        var parameters = config.toParameters();
        return businessFacade.nodeSimilarity(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new NodeSimilarityStatsResultTransformer(
                similarityDistributionInstructions.shouldComputeDistribution(),
                config.toMap()
            )
        ).join();
    }

    public Stream<SimilarityStatsResult> filteredNodeSimilarity(
        String graphName,
        Map<String, Object> configuration

    ) {
        var config = configurationParser.parseConfiguration(configuration, FilteredNodeSimilarityStatsConfig::of);

        var parameters = config.toFilteredParameters();
        return businessFacade.filteredNodeSimilarity(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new NodeSimilarityStatsResultTransformer(
                similarityDistributionInstructions.shouldComputeDistribution(),
                config.toMap()
            )
        ).join();
    }

}
