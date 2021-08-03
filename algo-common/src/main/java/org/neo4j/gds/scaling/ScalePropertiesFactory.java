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
package org.neo4j.gds.scaling;

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;

public final class ScalePropertiesFactory<CONFIG extends ScalePropertiesBaseConfig> extends AbstractAlgorithmFactory<ScaleProperties, CONFIG> {

    public ScalePropertiesFactory() {
        super();
    }

    @Override
    protected long taskVolume(Graph graph, CONFIG configuration) {
        return 0;
    }

    @Override
    protected String taskName() {
        return "ScaleProperties";
    }

    @Override
    protected ScaleProperties build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        return new ScaleProperties(
            graph,
            configuration,
            tracker,
            Pools.DEFAULT
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
}
