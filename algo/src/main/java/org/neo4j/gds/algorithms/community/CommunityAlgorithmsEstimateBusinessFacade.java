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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutMemoryEstimateDefinition;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringMemoryEstimateDefinition;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMemoryEstimateDefinition;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansMemoryEstimateDefinition;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMemoryEstimateDefinition;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenMemoryEstimateDefinition;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainMemoryEstimateDefinition;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculatorMemoryEstimateDefinition;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMemoryEstimateDefinition;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.scc.SccMemoryEstimateDefinition;
import org.neo4j.gds.triangle.IntersectingTriangleCountMemoryEstimateDefinition;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMemoryEstimateDefinition;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccMemoryEstimateDefinition;

import java.util.Optional;

public class CommunityAlgorithmsEstimateBusinessFacade {

    private final AlgorithmEstimator algorithmEstimator;

    public CommunityAlgorithmsEstimateBusinessFacade(
        AlgorithmEstimator algorithmEstimator
    ) {
        this.algorithmEstimator = algorithmEstimator;
    }

    public <C extends WccBaseConfig> MemoryEstimateResult wcc(Object graphNameOrConfiguration, C configuration) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new WccMemoryEstimateDefinition(configuration.isIncremental())
        );
    }


    public <C extends ApproxMaxKCutBaseConfig> MemoryEstimateResult approxMaxKCut(Object graphNameOrConfiguration, C configuration) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new ApproxMaxKCutMemoryEstimateDefinition(configuration.toMemoryEstimationParameters())
        );
    }

    public <C extends K1ColoringBaseConfig> MemoryEstimateResult k1Coloring(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new K1ColoringMemoryEstimateDefinition()
        );
    }

    public <C extends KCoreDecompositionBaseConfig> MemoryEstimateResult kcore(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new KCoreDecompositionMemoryEstimateDefinition()
        );
    }

    public <C extends KmeansBaseConfig> MemoryEstimateResult kmeans(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new KmeansMemoryEstimateDefinition(configuration.toParameters())
        );
    }

    public <C extends LabelPropagationBaseConfig> MemoryEstimateResult labelPropagation(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new LabelPropagationMemoryEstimateDefinition()
        );
    }
    public <C extends TriangleCountBaseConfig> MemoryEstimateResult triangleCount(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new IntersectingTriangleCountMemoryEstimateDefinition()
        );
    }


    public <C extends LeidenBaseConfig> MemoryEstimateResult leiden(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new LeidenMemoryEstimateDefinition(configuration.toMemoryEstimationParameters())
        );
    }

    public <C extends LouvainBaseConfig> MemoryEstimateResult louvain(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new LouvainMemoryEstimateDefinition(configuration.toMemoryEstimationParameters())
        );
    }

    public <C extends SccBaseConfig> MemoryEstimateResult estimateScc(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new SccMemoryEstimateDefinition()
        );
    }

    public <C extends LocalClusteringCoefficientBaseConfig> MemoryEstimateResult localClusteringCoefficient(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new LocalClusteringCoefficientMemoryEstimateDefinition(configuration.seedProperty())
        );
    }

    public <C extends ModularityBaseConfig> MemoryEstimateResult modularity(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new ModularityCalculatorMemoryEstimateDefinition()
        );
    }

    public <C extends ModularityOptimizationBaseConfig> MemoryEstimateResult modularityOptimization(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new ModularityOptimizationMemoryEstimateDefinition()
        );
    }
}
