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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

/**
 * @deprecated Used in Pregel things which will go away, do not add new usages.
 */
@Deprecated(forRemoval = true)
public abstract class WriteProc<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends WritePropertyConfig & AlgoBaseConfig> extends NodePropertiesWriter<ALGO, ALGO_RESULT, CONFIG, PROC_RESULT> {

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult,
        ExecutionContext executionContext
    );

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Write procedures must implement either `nodeProperties` or `nodePropertyList`.");
    }

    protected List<NodeProperty> nodePropertyList(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        return List.of(ImmutableNodeProperty.of(computationResult.config().writeProperty(), nodeProperties(computationResult)));
    }

    protected Stream<PROC_RESULT> write(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult) {
        return computationResultConsumer().consume(
            computeResult,
            executionContext()
        );
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder);
    }

    @Override
    public WriteNodePropertiesComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, PROC_RESULT> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            this::nodePropertyList,
            name()
        );
    }
}
