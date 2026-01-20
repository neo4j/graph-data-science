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
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.centrality.ArticulationPointsMutateStep;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointsMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public class ArticulationPointsMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<ArticulationPointsResult>, Stream<ArticulationPointsMutateResult>> {
    private final Graph graph;
    private final GraphStore graphStore;
    private final Map<String, Object> configuration;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;

    public ArticulationPointsMutateResultTransformer(
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty
    ) {
        this.graph = graph;
        this.graphStore = graphStore;
        this.configuration = configuration;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
    }

    @Override
    public Stream<ArticulationPointsMutateResult> apply(TimedAlgorithmResult<ArticulationPointsResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();
        var mutateStep  = new ArticulationPointsMutateStep(
            mutateNodePropertyService,
            mutateProperty,
            labelsToUpdate
        );

        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            result
        );

        return Stream.of(
            new ArticulationPointsMutateResult(
                result.articulationPoints().cardinality(),
                mutateMetadata.nodePropertiesWritten().value(),
                mutateMetadata.mutateMillis(),
                timedAlgorithmResult.computeMillis(),
                configuration
            )
        );
    }

}
