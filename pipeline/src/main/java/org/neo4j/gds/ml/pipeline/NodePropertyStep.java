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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.executor.AlgoConfigParser;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallableFinder;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.executor.ProcedureExecutorSpec;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_NODE_LABELS;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_RELATIONSHIP_TYPES;

public final class NodePropertyStep implements ExecutableNodePropertyStep {
    private final GdsCallableFinder.GdsCallableDefinition callableDefinition;
    private final Map<String, Object> config;

    private final List<String> contextNodeLabels;

    private final List<String> contextRelationshipTypes;

    public NodePropertyStep(
        GdsCallableFinder.GdsCallableDefinition callableDefinition,
        Map<String, Object> config
    ) {
        this(callableDefinition, config, List.of(), List.of());
    }
    public NodePropertyStep(
        GdsCallableFinder.GdsCallableDefinition callableDefinition,
        Map<String, Object> config,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes
    ) {
        this.callableDefinition = callableDefinition;
        this.config = config;
        this.contextNodeLabels = contextNodeLabels;
        this.contextRelationshipTypes = contextRelationshipTypes;
    }

    @Override
    public Map<String, Object> config() {
        return config;
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
    public String mutateNodeProperty() {
        return config().get(MUTATE_PROPERTY_KEY).toString();
    }

    @Override
    public String procName() {
        return callableDefinition.name();
    }

    @Override
    public String rootTaskName() {
        return callableDefinition.algorithmSpec().algorithmFactory().taskName();
    }

    @Override
    public MemoryEstimation estimate(ModelCatalog modelCatalog, String username, List<String> nodeLabels, List<String> relTypes)  {
        var algoSpec = getAlgorithmSpec(modelCatalog);

        var configCopy = new HashMap<>(config);
        configCopy.put("relationshipTypes", relTypes);
        configCopy.put("nodeLabels", nodeLabels);

        // no defaults nor limits when considering memory usage
        var defaults = DefaultsConfiguration.Empty;
        var limits = LimitsConfiguration.Empty;

        var algoConfig = new AlgoConfigParser<>(username, algoSpec.newConfigFunction(), defaults, limits).processInput(configCopy);

        try {
            algoSpec.algorithmFactory().memoryEstimation(algoConfig);
        } catch (MemoryEstimationNotImplementedException exception) {
            // If a single node property step cannot be estimated, we ignore this step in the estimation
            return MemoryEstimations.of(callableDefinition.name(), MemoryRange.of(0));
        }

        return algoSpec.algorithmFactory().memoryEstimation(algoConfig);
    }

    @Override
    public void execute(
        ExecutionContext executionContext,
        String graphName,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relTypes
    ) {
        var configCopy = new HashMap<>(config);
        var nodeLabelStrings = nodeLabels.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        var relTypeStrings = relTypes.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        configCopy.put("nodeLabels", nodeLabelStrings);
        configCopy.put("relationshipTypes", relTypeStrings);

        var algorithmSpec = getAlgorithmSpec(executionContext.modelCatalog());

        new ProcedureExecutor<>(
            algorithmSpec,
            new ProcedureExecutorSpec<>(),
            executionContext
        ).compute(graphName, configCopy);
    }

    private AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>> getAlgorithmSpec(ModelCatalog modelCatalog) {
        return callableDefinition
            .algorithmSpec()
            .withModelCatalog(modelCatalog);
    }

    @Override
    public Map<String, Object> toMap() {
        var configWithContext = new LinkedHashMap<>(config);
        configWithContext.put(CONTEXT_NODE_LABELS, contextNodeLabels);
        configWithContext.put(CONTEXT_RELATIONSHIP_TYPES, contextRelationshipTypes);

        return Map.of("name", procName(), "config", configWithContext);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodePropertyStep that = (NodePropertyStep) o;
        return Objects.equals(callableDefinition, that.callableDefinition) && Objects.equals(
            config,
            that.config
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(callableDefinition, config);
    }
}
