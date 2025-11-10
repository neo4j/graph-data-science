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
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.community.K1ColoringMutateStep;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class K1ColoringMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<K1ColoringResult>, Stream<K1ColoringMutateResult>> {

    private final Map<String, Object> configuration;
    private final boolean computeUsedColors;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;


    public K1ColoringMutateResultTransformer(
        Map<String, Object> configuration,
        boolean computeUsedColors,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        Graph graph,
        GraphStore graphStore
    ) {
        this.configuration = configuration;
        this.computeUsedColors = computeUsedColors;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
    }

    @Override
    public Stream<K1ColoringMutateResult> apply(TimedAlgorithmResult<K1ColoringResult> timedAlgorithmResult) {

        var k1ColoringResult = timedAlgorithmResult.result();

        var mutateStep = new K1ColoringMutateStep(mutateNodePropertyService,labelsToUpdate,mutateProperty);
        var  mutateMillis = mutate(k1ColoringResult,mutateStep);

        var usedColors = (computeUsedColors) ? k1ColoringResult.usedColors().cardinality() : 0;

        var k1ColoringMutateResult =  new K1ColoringMutateResult(
            0,
            timedAlgorithmResult.computeMillis(),
            mutateMillis,
            k1ColoringResult.colors().size(),
            usedColors,
            k1ColoringResult.ranIterations(),
            k1ColoringResult.didConverge(),
            configuration
        );

        return Stream.of(k1ColoringMutateResult);

    }

    long mutate(K1ColoringResult k1ColoringResult,MutateStep<K1ColoringResult,Void> mutateStep){
        var mutateMillis = new AtomicLong();
        try (var ignored = ProgressTimer.start(mutateMillis::set)) {
             mutateStep.execute(
                graph,
                graphStore,
                 k1ColoringResult
            );
        }
        return mutateMillis.get();

    }
}
