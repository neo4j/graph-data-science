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

import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.MutateNodePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_NODE_LABELS;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_RELATIONSHIP_TYPES;

/**
 * A node property step backed by a remote (python-runtime) algorithm. The remote algorithm manages its own
 * graph scoping, but the pipeline still passes down the node labels and relationship types the step should
 * run on (for link prediction that is the {@code _FEATURE_INPUT_} subgraph).
 */
final class RemoteNodePropertyStep implements ExecutableNodePropertyStep {
    private final String procName;
    private final Map<String, Object> configuration;
    private final List<String> contextNodeLabels;
    private final List<String> contextRelationshipTypes;
    private final RemoteNodePropertyStepExecutor remoteExecutor;

    RemoteNodePropertyStep(
        String procName,
        Map<String, Object> configuration,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes,
        RemoteNodePropertyStepExecutor remoteExecutor
    ) {
        this.procName = procName;
        this.configuration = configuration;
        this.contextNodeLabels = contextNodeLabels;
        this.contextRelationshipTypes = contextRelationshipTypes;
        this.remoteExecutor = remoteExecutor;
    }

    @Override
    public void execute(
        ExecutionContext executionContext,
        String graphName,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relTypes,
        Concurrency trainConcurrency,
        Stub stub
    ) {
        var nodeLabelStrings = nodeLabels.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        var relTypeStrings = relTypes.stream().map(ElementIdentifier::name).collect(Collectors.toList());

        remoteExecutor.execute(
            procName,
            graphName,
            new HashMap<>(configuration),
            nodeLabelStrings,
            relTypeStrings,
            executionContext.terminationMonitor()
        );
    }

    @Override
    public Map<String, Object> config() {
        return configuration;
    }

    @Override
    public List<String> contextNodeLabels() {
        return contextNodeLabels;
    }

    @Override
    public List<String> contextRelationshipTypes() {
        return contextRelationshipTypes;
    }

    @Override
    public String procName() {
        return procName;
    }

    @Override
    public String mutateNodeProperty() {
        return config().get(MUTATE_PROPERTY_KEY).toString();
    }

    @Override
    public MemoryEstimation estimate(
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        ModelCatalog modelCatalog,
        String username,
        List<String> nodeLabels,
        List<String> relTypes,
        Stub stub
    ) {
        // compute happens remotely, nothing is allocated in this JVM for the step itself
        return MemoryEstimations.of(procName, MemoryRange.of(0));
    }

    @Override
    public Map<String, Object> toMap() {
        var configWithContext = new LinkedHashMap<>(configuration);
        configWithContext.put(CONTEXT_NODE_LABELS, contextNodeLabels);
        configWithContext.put(CONTEXT_RELATIONSHIP_TYPES, contextRelationshipTypes);

        return Map.of("name", procName(), "config", configWithContext);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteNodePropertyStep that = (RemoteNodePropertyStep) o;
        return Objects.equals(procName, that.procName) && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(procName, configuration);
    }
}
