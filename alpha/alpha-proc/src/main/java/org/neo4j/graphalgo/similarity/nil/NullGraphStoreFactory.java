/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.similarity.nil;

import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;

public class NullGraphStoreFactory extends GraphStoreFactory<GraphCreateConfig> {

    NullGraphStoreFactory() {
        super(null, null, null);
    }

    @Override
    public ImportResult build() {
        return ImportResult.of(GraphDimensions.of(0L), new NullGraphStore());
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        throw new MemoryEstimationNotImplementedException();
    }

    @Override
    protected ProgressLogger initProgressLogger() {
        return ProgressLogger.NULL_LOGGER;
    }
}
