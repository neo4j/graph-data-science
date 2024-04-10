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
package org.neo4j.gds.procedures.misc.scaleproperties;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteResult;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public final class ScalePropertiesWriteResult extends StandardWriteResult {

    public final long nodePropertiesWritten;
    public final Map<String, Map<String, List<Double>>> scalerStatistics;

    public ScalePropertiesWriteResult(
        Map<String, Map<String, List<Double>>> scalerStatistics,
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
        this.scalerStatistics = scalerStatistics;
    }

    public static class Builder extends AbstractResultBuilder<ScalePropertiesWriteResult> {

        private Map<String, Map<String, List<Double>>> scalerStatistics;

        public Builder withScalerStatistics(Map<String, Map<String, List<Double>>> stats) {
            this.scalerStatistics = stats;
            return this;
        }

        @Override
        public ScalePropertiesWriteResult build() {
            return new ScalePropertiesWriteResult(
                scalerStatistics,
                preProcessingMillis,
                computeMillis,
                writeMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
