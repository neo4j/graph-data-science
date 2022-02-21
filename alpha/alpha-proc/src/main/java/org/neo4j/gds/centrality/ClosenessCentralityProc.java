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
package org.neo4j.gds.centrality;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.closeness.ClosenessCentralityConfig;
import org.neo4j.gds.impl.closeness.MSClosenessCentrality;

public abstract class ClosenessCentralityProc<PROC_RESULT> extends NodePropertiesWriter<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig, PROC_RESULT> {

    protected static final String DESCRIPTION =
        "Closeness centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";


    @Override
    protected ClosenessCentralityConfig newConfig(String username, CypherMapWrapper config) {
        return ClosenessCentralityConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<MSClosenessCentrality, ClosenessCentralityConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "ClosenessCentrality";
            }

            @Override
            public MSClosenessCentrality build(
                Graph graph,
                ClosenessCentralityConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                return new MSClosenessCentrality(
                    graph,
                    configuration.concurrency(),
                    configuration.improved(),
                    allocationTracker,
                    Pools.DEFAULT,
                    progressTracker
                );
            }
        };
    }
}
