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
package org.neo4j.gds.algorithms.centrality;

import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityMemoryEstimateDefinition;
import org.neo4j.gds.degree.DegreeCentralityAlgorithmEstimateDefinition;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.influenceMaximization.CELFMemoryEstimateDefinition;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankMemoryEstimateDefinition;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Optional;

public class CentralityAlgorithmsEstimateBusinessFacade {

    private final AlgorithmEstimator algorithmEstimator;

    public CentralityAlgorithmsEstimateBusinessFacade(
        AlgorithmEstimator algorithmEstimator
    ) {
        this.algorithmEstimator = algorithmEstimator;
    }

    public <C extends BetweennessCentralityBaseConfig> MemoryEstimateResult betweennessCentrality(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new BetweennessCentralityMemoryEstimateDefinition(configuration.hasRelationshipWeightProperty())
        );
    }

    public <C extends DegreeCentralityConfig> MemoryEstimateResult degreeCentrality(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new DegreeCentralityAlgorithmEstimateDefinition(configuration.hasRelationshipWeightProperty())

        );
    }

    public <C extends InfluenceMaximizationBaseConfig> MemoryEstimateResult celf(
        Object graphNameOrConfiguration,
        C configuration
    ) {

        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new CELFMemoryEstimateDefinition(configuration.toParameters())
        );
    }

    public <C extends PageRankConfig> MemoryEstimateResult pageRank(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return pageRankVariant(graphNameOrConfiguration, configuration);
    }

    public <C extends PageRankConfig> MemoryEstimateResult articleRank(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return pageRankVariant(graphNameOrConfiguration, configuration);
    }

    public <C extends PageRankConfig> MemoryEstimateResult eigenvector(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return pageRankVariant(graphNameOrConfiguration, configuration);
    }

    private <C extends PageRankConfig> MemoryEstimateResult pageRankVariant(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new PageRankMemoryEstimateDefinition()
        );
    }
}
