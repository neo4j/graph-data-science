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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ScalePropertiesWriteProc extends BaseProc {

    @Context
    public NodePropertyExporterBuilder nodePropertyExporterBuilder;

    @Procedure(value = "gds.scaleProperties.write", mode = WRITE)
    @Description(SCALE_PROPERTIES_DESCRIPTION)
    public Stream<ScalePropertiesWriteProc.WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new ScalePropertiesWriteSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(value = "gds.scaleProperties.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var spec = new ScalePropertiesWriteSpec();

        return new MemoryEstimationExecutor<>(
            spec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder);
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends StandardWriteResult {

        public final long nodePropertiesWritten;
        public final Map<String, Map<String, List<Double>>> scalerStatistics;

        WriteResult(
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

        static class Builder extends AbstractResultBuilder<WriteResult> {

            private Map<String, Map<String, List<Double>>> scalerStatistics;

            Builder withScalerStatistics(Map<String, Map<String, List<Double>>> stats) {
                this.scalerStatistics = stats;
                return this;
            }

            @Override
            public WriteResult build() {
                return new WriteResult(
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
}
