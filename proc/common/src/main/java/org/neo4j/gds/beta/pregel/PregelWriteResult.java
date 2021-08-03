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
package org.neo4j.gds.beta.pregel;

import org.neo4j.gds.results.StandardWriteResult;

import java.util.Map;

@SuppressWarnings("unused")
public final class PregelWriteResult extends StandardWriteResult {

    public final long nodePropertiesWritten;
    public final long ranIterations;
    public final boolean didConverge;

    private PregelWriteResult(
        long nodePropertiesWritten,
        long createMillis,
        long computeMillis,
        long writeMillis,
        long ranIterations,
        boolean didConverge,
        Map<String, Object> configuration
    ) {
        super(createMillis, computeMillis, 0L, writeMillis, configuration);
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
    }

    public static class Builder extends AbstractPregelResultBuilder<PregelWriteResult> {

        @Override
        public PregelWriteResult build() {
            return new PregelWriteResult(
                nodePropertiesWritten,
                createMillis,
                computeMillis,
                writeMillis,
                ranIterations,
                didConverge,
                config.toMap()
            );
        }
    }
}
