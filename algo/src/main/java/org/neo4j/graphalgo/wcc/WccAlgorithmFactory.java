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
package org.neo4j.graphalgo.wcc;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;

public final class WccAlgorithmFactory<CONFIG extends WccBaseConfig> extends AbstractAlgorithmFactory<Wcc, CONFIG> {

    public WccAlgorithmFactory() {
        super();
    }

    @Override
    protected long taskVolume(Graph graph, CONFIG configuration) {
        return graph.relationshipCount();
    }

    @Override
    protected String taskName() {
        return "WCC";
    }

    @Override
    protected Wcc build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, ProgressLogger progressLogger
    ) {
        if (configuration.hasRelationshipWeightProperty() && configuration.threshold() == 0) {
            progressLogger
                .getLog()
                .warn("Specifying a `relationshipWeightProperty` has no effect unless `threshold` is also set.");
        }
        return new Wcc(
            graph,
            Pools.DEFAULT,
            ParallelUtil.DEFAULT_BATCH_SIZE,
            configuration,
            progressLogger,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return Wcc.memoryEstimation(config.isIncremental());
    }

    @TestOnly
    WccAlgorithmFactory(ProgressLogger.ProgressLoggerFactory factory) {
        super(factory);
    }
}
