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
import org.neo4j.gds.community.LccMutateStep;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.community.LocalClusteringCoefficientMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public class LccMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<LocalClusteringCoefficientResult>, Stream<LocalClusteringCoefficientMutateResult>> {

    private final Map<String, Object> configuration;
    private final long nodeCount;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;

    public LccMutateResultTransformer(
        Map<String, Object> configuration,
        long nodeCount,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        Graph graph,
        GraphStore graphStore
    ) {
        this.configuration = configuration;
        this.nodeCount = nodeCount;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
    }

    @Override
    public Stream<LocalClusteringCoefficientMutateResult> apply(TimedAlgorithmResult<LocalClusteringCoefficientResult> timedAlgorithmResult) {

        var localClusteringCoefficientResult = timedAlgorithmResult.result();

        var mutateStep = new LccMutateStep(mutateNodePropertyService, labelsToUpdate, mutateProperty);
        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            localClusteringCoefficientResult
        );

        var localClusteringCoefficientMutateResult = new LocalClusteringCoefficientMutateResult(
            localClusteringCoefficientResult.averageClusteringCoefficient(),
            nodeCount,
            0,
            timedAlgorithmResult.computeMillis(),
            mutateMetadata.mutateMillis(),
            mutateMetadata.nodePropertiesWritten().value(),
            configuration
        );

        return Stream.of(localClusteringCoefficientMutateResult);
    }
}
