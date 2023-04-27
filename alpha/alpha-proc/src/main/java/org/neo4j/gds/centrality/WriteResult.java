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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.Map;

@SuppressWarnings("unused")
public final class WriteResult  {

    public final long nodes;
    public final String writeProperty;
    public final long writeMillis;
    public final long computeMillis;
    public final long preProcessingMillis;
    public final Map<String, Object> centralityDistribution;

    WriteResult(
        long nodes,
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        String writeProperty,
        @Nullable Map<String, Object> centralityDistribution
    ) {
        this.preProcessingMillis=preProcessingMillis;
        this.computeMillis=computeMillis;
        this.writeMillis=writeMillis;

        this.writeProperty = writeProperty;
        this.centralityDistribution = centralityDistribution;
        this.nodes = nodes;
    }

    static final class Builder extends AbstractCentralityResultBuilder<WriteResult> {
        public String writeProperty;

         Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        public Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }

        @Override
        public Builder withNodePropertiesWritten(long nodePropertiesWritten){
            return this;
        }


        @Override
        public WriteResult buildResult() {
            return new WriteResult(
                nodeCount,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                writeProperty,
                centralityHistogram
            );
        }
    }
}
