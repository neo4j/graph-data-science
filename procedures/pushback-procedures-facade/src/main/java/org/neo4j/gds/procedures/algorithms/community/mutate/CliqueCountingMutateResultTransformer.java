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
package org.neo4j.gds.procedures.algorithms.community.mutate;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.community.CliqueCountingMutateStep;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public class CliqueCountingMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<CliqueCountingResult>, Stream<CliqueCountingMutateResult>> {

    private final Map<String, Object> configuration;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;

    public CliqueCountingMutateResultTransformer(
        Map<String, Object> configuration,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty, Graph graph, GraphStore graphStore
    ) {
        this.configuration = configuration;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
    }

    @Override
    public Stream<CliqueCountingMutateResult> apply(TimedAlgorithmResult<CliqueCountingResult> timedAlgorithmResult) {
        var cliqueCountingResult = timedAlgorithmResult.result();

        var mutateStep = new CliqueCountingMutateStep(mutateNodePropertyService, labelsToUpdate, mutateProperty);
        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            cliqueCountingResult
        );

        var cliqueCountingMutateResult = new CliqueCountingMutateResult(
            0,
            timedAlgorithmResult.computeMillis(),
            mutateMetadata.mutateMillis(),
            mutateMetadata.nodePropertiesWritten().value(),
            Arrays.stream(cliqueCountingResult.globalCount()).boxed().toList(),
            configuration
        );

        return Stream.of(cliqueCountingMutateResult);
    }
}
