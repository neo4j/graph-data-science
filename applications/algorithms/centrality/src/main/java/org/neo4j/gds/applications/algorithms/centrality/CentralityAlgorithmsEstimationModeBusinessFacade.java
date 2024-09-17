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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsMemoryEstimateDefinition;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityMemoryEstimateDefinition;
import org.neo4j.gds.bridges.BridgesBaseConfig;
import org.neo4j.gds.bridges.BridgesMemoryEstimateDefinition;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.degree.DegreeCentralityAlgorithmEstimateDefinition;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.influenceMaximization.CELFMemoryEstimateDefinition;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.pagerank.PageRankMemoryEstimateDefinition;
import org.neo4j.gds.pagerank.RankConfig;

public class CentralityAlgorithmsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public CentralityAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimation articulationPoints() {
        return new ArticulationPointsMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult articulationPoints(
        ArticulationPointsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = articulationPoints();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }


    public MemoryEstimation betweennessCentrality(RelationshipWeightConfig configuration) {
        return new BetweennessCentralityMemoryEstimateDefinition(configuration.hasRelationshipWeightProperty()).memoryEstimation();
    }

    public MemoryEstimateResult betweennessCentrality(
        BetweennessCentralityBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = betweennessCentrality(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }
     MemoryEstimation bridges() {
        return new BridgesMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult bridges(BridgesBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = bridges();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation celf(InfluenceMaximizationBaseConfig configuration) {
        return new CELFMemoryEstimateDefinition(configuration.toParameters()).memoryEstimation();
    }

    public MemoryEstimateResult celf(InfluenceMaximizationBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = celf(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation closenessCentrality(ClosenessCentralityBaseConfig ignored) {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation degreeCentrality(RelationshipWeightConfig configuration) {
        return new DegreeCentralityAlgorithmEstimateDefinition(configuration.hasRelationshipWeightProperty()).memoryEstimation();
    }

    public MemoryEstimateResult degreeCentrality(
        DegreeCentralityConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = degreeCentrality(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation harmonicCentrality() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation pageRank() {
        return new PageRankMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult pageRank(RankConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = pageRank();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

}
