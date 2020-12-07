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
package org.neo4j.graphalgo.beta.pregel;

public final class PregelWriteResult {
    public final long nodePropertiesWritten;
    public final long createMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long ranIterations;
    public final boolean didConverge;

    private PregelWriteResult(
        long nodePropertiesWritten,
        long createMillis,
        long computeMillis,
        long writeMillis,
        long ranIterations,
        boolean didConverge
    ) {
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.createMillis = createMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
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
                didConverge
            );
        }
    }
}
