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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;

import java.util.Map;
import java.util.stream.Stream;

public class LocalFilteredNodeSimilarityMutateStub implements FilteredNodeSimilarityMutateStub {

    private final GenericStub genericStub;
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final SimilarityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade;
    private final ProcedureReturnColumns procedureReturnColumns;

    LocalFilteredNodeSimilarityMutateStub(
        GenericStub genericStub,
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        SimilarityAlgorithmsMutateModeBusinessFacade mutateModeBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.genericStub = genericStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.mutateModeBusinessFacade = mutateModeBusinessFacade;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    @Override
    public FilteredNodeSimilarityMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(FilteredNodeSimilarityMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            FilteredNodeSimilarityMutateConfig::of,
            estimationModeBusinessFacade::filteredNodeSimilarity
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            FilteredNodeSimilarityMutateConfig::of,
            estimationModeBusinessFacade::filteredNodeSimilarity
        );
    }

    @Override
    public Stream<SimilarityMutateResult> execute(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForMutateMode();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            FilteredNodeSimilarityMutateConfig::of,
            (graphName, configuration, __) -> mutateModeBusinessFacade.filteredNodeSimilarity(
                graphName,
                configuration,
                resultBuilder,
                procedureReturnColumns.contains("similarityDistribution")
            ),
            resultBuilder
        );
    }


}
