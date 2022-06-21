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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Collection;

public interface NodePropertyStepExecutor {
    NodePropertyStepExecutor NOOP = loggingOnly(ProgressTracker.NULL_TRACKER);

    static NodePropertyStepExecutor loggingOnly(ProgressTracker progressTracker) {
        return new LoggingOnlyNodePropertyExecutor(progressTracker);
    }

    static <CONFIG extends AlgoBaseConfig & GraphNameConfig> NodePropertyStepExecutor of(
        ExecutionContext executionContext,
        GraphStore graphStore,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        return of(executionContext, graphStore, config, config.internalRelationshipTypes(graphStore), progressTracker);

    }
    static <CONFIG extends AlgoBaseConfig & GraphNameConfig> NodePropertyStepExecutor of(
        ExecutionContext executionContext,
        GraphStore graphStore,
        CONFIG config,
        Collection<RelationshipType> relationshipTypes,
        ProgressTracker progressTracker
    ) {
        return new NodePropertyStepExecutorImpl<>(
            executionContext,
            config,
            graphStore,
            relationshipTypes,
            progressTracker
        );
    }

    void executeNodePropertySteps(Pipeline<?> pipeline);
}
