/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.logging.Log;

public class EigenvectorCentralityFactory extends AlgorithmFactory<EigenvectorCentrality, EigenvectorCentralityConfig> {

    @Override
    public EigenvectorCentrality build(
        final Graph graph,
        final EigenvectorCentralityConfig configuration,
        final AllocationTracker tracker,
        final Log log
    ) {
        return new EigenvectorCentrality(
            graph,
            configuration,
            Pools.DEFAULT,
            log,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(EigenvectorCentralityConfig config) {
        //TODO
        return null;
    }
}
