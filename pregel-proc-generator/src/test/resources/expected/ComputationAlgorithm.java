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
package org.neo4j.graphalgo.beta.pregel.cc;

import javax.annotation.processing.Generated;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

@Generated("org.neo4j.graphalgo.beta.pregel.PregelProcessor")
public final class ComputationAlgorithm extends Algorithm<ComputationAlgorithm, PregelResult> {
    private final Pregel<PregelProcedureConfig> pregelJob;

    ComputationAlgorithm(Graph graph, PregelProcedureConfig configuration,
                         AllocationTracker tracker, ProgressLogger progressLogger) {
        this.pregelJob = Pregel.create(graph, configuration, new Computation(), Pools.DEFAULT, tracker, progressLogger);
    }

    @Override
    public PregelResult compute() {
        return pregelJob.run();
    }

    @Override
    public ComputationAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
        pregelJob.release();
    }
}
