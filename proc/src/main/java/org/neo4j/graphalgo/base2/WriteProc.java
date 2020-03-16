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
package org.neo4j.graphalgo.base2;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;

import java.util.stream.Stream;

public abstract class WriteProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends AlgoBaseConfig> extends WriteOrMutateProc<ALGO, ALGO_RESULT, PROC_RESULT, CONFIG> {

    protected Stream<PROC_RESULT> write(ComputationResult2<ALGO, ALGO_RESULT, CONFIG> computeResult) {
        return writeOrMutate(
            computeResult,
            (writeBuilder, computationResult) -> writeNodeProperties(writeBuilder, computationResult)
        );
    }

    private void writeNodeProperties(
        AbstractResultBuilder<?> writeBuilder,
        ComputationResult2<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        PropertyTranslator<ALGO_RESULT> resultPropertyTranslator = nodePropertyTranslator(computationResult);

        CONFIG config = computationResult.config();
        if (!(config instanceof WritePropertyConfig)) {
            throw new IllegalArgumentException(String.format(
                "Can only write results if the config implements %s.",
                WritePropertyConfig.class
            ));
        }

        WritePropertyConfig writePropertyConfig = (WritePropertyConfig) config;
        try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
            log.debug("Writing results");

            Graph graph = computationResult.graph();
            TerminationFlag terminationFlag = computationResult.algorithm().getTerminationFlag();
            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, writePropertyConfig.writeConcurrency())
                .build();

            exporter.write(
                writePropertyConfig.writeProperty(),
                computationResult.result(),
                resultPropertyTranslator
            );
            writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }
    }
}
