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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardWriteResult;

import java.util.Map;

public final class WriteResult extends StandardWriteResult {

    public final long nodePropertiesWritten;

    WriteResult(
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            0L,
            writeMillis,
            configuration
        );
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static class Builder extends AbstractResultBuilder<WriteResult> {

        @Override
        public WriteResult build() {
            return new WriteResult(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
