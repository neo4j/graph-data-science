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
package org.neo4j.gds;

import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.logging.Log;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public abstract class MutateComputationResultConsumer<ALGO extends Algorithm<ALGO, ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutateConfig, RESULT>
    implements ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, Stream<RESULT>> {

    public interface ResultBuilderFunction<ALGO extends Algorithm<ALGO, ALGO_RESULT>, ALGO_RESULT, CONFIG extends MutateConfig, RESULT> {
        AbstractResultBuilder<RESULT> apply(AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult);
    }

    private final ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction;
    protected final Log log;

    protected MutateComputationResultConsumer(ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction, Log log) {
        this.resultBuilderFunction = resultBuilderFunction;
        this.log = log;
    }

    @Override
    public Stream<RESULT> consume(AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return runWithExceptionLogging("Graph mutation failed", log, () -> {
            CONFIG config = computationResult.config();

            AbstractResultBuilder<RESULT> builder = resultBuilderFunction.apply(computationResult)
                .withCreateMillis(computationResult.createMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(config);

            if (!computationResult.isGraphEmpty()) {
                updateGraphStore(builder, computationResult);
                computationResult.graph().releaseProperties();
            }
            return Stream.of(builder.build());
        });
    }

    protected abstract void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    );
}
