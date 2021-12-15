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
package org.neo4j.gds.shortestpath;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.ShortestPathDeltaStepping;
import org.neo4j.gds.utils.InputNodeValidator;

/**
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 */
public abstract class ShortestPathDeltaSteppingProc<PROC_RESULT> extends NodePropertiesWriter<ShortestPathDeltaStepping, ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig, PROC_RESULT> {

    protected static final String DESCRIPTION = "Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm.";

    @Override
    protected ShortestPathDeltaSteppingConfig newConfig(String username, CypherMapWrapper config) {
        return ShortestPathDeltaSteppingConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "ShortestPathDeltaStepping";
            }

            @Override
            public ShortestPathDeltaStepping build(
                Graph graph,
                ShortestPathDeltaSteppingConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                InputNodeValidator.validateStartNode(configuration.startNode(), graph);
                return new ShortestPathDeltaStepping(
                    graph,
                    configuration.startNode(),
                    configuration.delta(),
                    progressTracker
                );
            }
        };
    }
}
