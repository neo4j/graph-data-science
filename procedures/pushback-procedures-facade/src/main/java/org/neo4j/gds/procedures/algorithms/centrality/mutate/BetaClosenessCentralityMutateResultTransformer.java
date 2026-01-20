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
package org.neo4j.gds.procedures.algorithms.centrality.mutate;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.centrality.BetaClosenessCentralityMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

    public class BetaClosenessCentralityMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<ClosenessCentralityResult>, Stream<BetaClosenessCentralityMutateResult>> {

    private final  GenericCentralityMutateResultTransformer<ClosenessCentralityResult> genericCentralityMutateResultTransformer;
    private final String mutateProperty;


    public BetaClosenessCentralityMutateResultTransformer(
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration,
        boolean shouldComputeDistribution,
        Concurrency concurrency,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty
    ) {
        genericCentralityMutateResultTransformer = new GenericCentralityMutateResultTransformer<>(
            graph,
            graphStore,
            configuration,
            shouldComputeDistribution,
            concurrency,
            mutateNodePropertyService,
            labelsToUpdate,
            mutateProperty
        );
        this.mutateProperty = mutateProperty;
    }

    @Override
    public Stream<BetaClosenessCentralityMutateResult> apply(TimedAlgorithmResult<ClosenessCentralityResult> timedAlgorithmResult) {

        var centralityMutateResult = genericCentralityMutateResultTransformer.apply(timedAlgorithmResult).findFirst().orElseThrow();

        return Stream.of(
                new BetaClosenessCentralityMutateResult(
                    centralityMutateResult.nodePropertiesWritten(),
                    centralityMutateResult.preProcessingMillis(),
                    centralityMutateResult.computeMillis(),
                    centralityMutateResult.postProcessingMillis(),
                    centralityMutateResult.mutateMillis(),
                    mutateProperty,
                    centralityMutateResult.centralityDistribution(),
                    centralityMutateResult.configuration()
                )
            );
    }
}
