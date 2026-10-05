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

import org.neo4j.gds.termination.TerminationMonitor;

import java.util.List;
import java.util.Map;

/**
 * Bridge that lets a training pipeline run a remote (python-runtime) algorithm as a node property step.
 * <p>
 * Implemented outside the open-source pipeline module (in the session), which is why it only speaks in
 * plain types. A {@link RemoteNodePropertyStep} is materialised at addNodeProperty time and persists in the
 * pipeline, so implementations must stay usable for the whole lifetime of the session.
 */
public interface RemoteNodePropertyStepExecutor {

    /**
     * @param procName canonical, lower-cased, {@code .mutate}-suffixed procedure name
     * @return whether this executor can run {@code procName} as a node property step
     */
    boolean handles(String procName);

    /**
     * Validates the step configuration at addNodeProperty time. Throw to reject the step.
     */
    default void validate(String procName, Map<String, Object> configuration) {}

    /**
     * Runs the remote algorithm and writes its result node property onto the in-session graph.
     * <p>
     * The {@code terminationMonitor} reflects the enclosing pipeline execution and must be observed while
     * waiting for the remote job, so that cancelling the pipeline also stops the remote computation.
     */
    void execute(
        String procName,
        String graphName,
        Map<String, Object> configuration,
        List<String> nodeLabels,
        List<String> relationshipTypes,
        TerminationMonitor terminationMonitor
    );

    /**
     * An executor that handles no procedure, used when no remote algorithm support is available.
     */
    RemoteNodePropertyStepExecutor EMPTY = new RemoteNodePropertyStepExecutor() {
        @Override
        public boolean handles(String procName) {
            return false;
        }

        @Override
        public void execute(
            String procName,
            String graphName,
            Map<String, Object> configuration,
            List<String> nodeLabels,
            List<String> relationshipTypes,
            TerminationMonitor terminationMonitor
        ) {
            throw new UnsupportedOperationException(
                "No remote node property step executor is available for `" + procName + "`"
            );
        }
    };
}
