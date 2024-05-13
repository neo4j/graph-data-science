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
package org.neo4j.gds.procedures.algorithms.pathfinding.stubs;

import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeMutateResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;

import java.util.Map;
import java.util.stream.Stream;

public class SpanningTreeMutateStub implements MutateStub<SpanningTreeMutateConfig, SpanningTreeMutateResult> {
    private final GenericStub genericStub;
    private final ApplicationsFacade applicationsFacade;

    public SpanningTreeMutateStub(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade
    ) {
        this.genericStub = genericStub;
        this.applicationsFacade = applicationsFacade;
    }

    @Override
    public SpanningTreeMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(SpanningTreeMutateConfig::of, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            username,
            configuration,
            SpanningTreeMutateConfig::of,
            __ -> estimationMode().spanningTree()
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            SpanningTreeMutateConfig::of,
            __ -> estimationMode().spanningTree()
        );
    }

    @Override
    public Stream<SpanningTreeMutateResult> execute(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SpanningTreeResultBuilderForMutateMode();

        return genericStub.execute(
            graphName,
            configuration,
            SpanningTreeMutateConfig::of,
            applicationsFacade.pathFinding().mutate()::spanningTree,
            resultBuilder
        );
    }

    private PathFindingAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.pathFinding().estimate();
    }
}
