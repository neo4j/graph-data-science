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

package org.neo4j.graphalgo.core;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.GraphSetupImpl;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.concurrent.ExecutorService;

@ValueClass
public interface ModernGraphLoader extends SharedGraphLoader {

    default ExecutorService executorService() {
        return Pools.DEFAULT;
    }

    String username();
    Log log();
    AllocationTracker tracker();
    Map<String, Object> params();
    TerminationFlag terminationFlag();
    GraphCreateConfig createConfig();

    @Override
    @Value.Lazy
    default GraphSetup toSetup() {
        return new GraphSetupImpl(
            params(),
            executorService(),
            log(),
            tracker(),
            terminationFlag(),
            createConfig()
        );
    }
}
