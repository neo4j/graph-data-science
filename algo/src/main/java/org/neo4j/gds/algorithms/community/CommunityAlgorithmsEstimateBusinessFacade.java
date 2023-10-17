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
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutAlgorithmFactory;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringAlgorithmFactory;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionAlgorithmFactory;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kmeans.KmeansAlgorithmFactory;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationFactory;
import org.neo4j.gds.leiden.LeidenAlgorithmFactory;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.louvain.LouvainAlgorithmFactory;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculatorFactory;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.scc.SccAlgorithmFactory;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.triangle.IntersectingTriangleCountFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientFactory;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccBaseConfig;

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
            new WccAlgorithmFactory<>()
        );
    }


    public <C extends ApproxMaxKCutBaseConfig> MemoryEstimateResult approxMaxKCut(Object graphNameOrConfiguration, C configuration) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new ApproxMaxKCutAlgorithmFactory<>()
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
            new K1ColoringAlgorithmFactory<>()
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
            new KCoreDecompositionAlgorithmFactory<>()
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
            new KmeansAlgorithmFactory<>()
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
            new LabelPropagationFactory<>()
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
            new IntersectingTriangleCountFactory<>()
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
            new LeidenAlgorithmFactory<>()
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
            new LouvainAlgorithmFactory<>()
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
            new SccAlgorithmFactory<>()
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
            new LocalClusteringCoefficientFactory<>()
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
            new ModularityCalculatorFactory<>()
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
            new ModularityOptimizationFactory<>()
        );
    }
}
