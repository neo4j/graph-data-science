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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.ExecutorService;

public final class LogScaler extends ScalarScaler {

    public static final String TYPE = "log";
    private static final String OFFSET_KEY = "offset";
    private final double offset;

    LogScaler(NodePropertyValues properties, double offset) {
        super(properties);
        this.offset = offset;
    }

    @Override
    public double scaleProperty(long nodeId) {
        return Math.log(properties.doubleValue(nodeId) + offset);
    }

    static ScalerFactory buildFrom(CypherMapWrapper mapWrapper) {
        mapWrapper.requireOnlyKeysFrom(List.of(OFFSET_KEY));
        final double offset = mapWrapper.getNumber(OFFSET_KEY, 0).doubleValue();
        return new ScalerFactory() {
            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public ScalarScaler create(
                NodePropertyValues properties,
                long nodeCount,
                int concurrency,
                ProgressTracker progressTracker,
                ExecutorService executor
            ) {
                return new LogScaler(properties, offset);
            }
        };
    }
}
