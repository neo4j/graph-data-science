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
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.CanonicalProcedureName;

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

class StubPoweredNodePropertyStep implements ExecutableNodePropertyStep {
    private final CanonicalProcedureName canonicalProcedureName;
    private final Map<String, Object> configuration;
    private final List<String> contextNodeLabels;
    private final List<String> contextRelationshipTypes;
    private final Algorithm algorithmMetadata;

    StubPoweredNodePropertyStep(
        CanonicalProcedureName canonicalProcedureName,
        Map<String, Object> configuration,
        List<String> contextNodeLabels,
        List<String> contextRelationshipTypes,
        Algorithm algorithmMetadata
    ) {
        this.canonicalProcedureName = canonicalProcedureName;
        this.configuration = configuration;
        this.contextNodeLabels = contextNodeLabels;
        this.contextRelationshipTypes = contextRelationshipTypes;
        this.algorithmMetadata = algorithmMetadata;
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
        var configCopy = new HashMap<>(configuration);
        var nodeLabelStrings = nodeLabels.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        var relTypeStrings = relTypes.stream().map(ElementIdentifier::name).collect(Collectors.toList());
        configCopy.put("nodeLabels", nodeLabelStrings);
        configCopy.put("relationshipTypes", relTypeStrings);
        configCopy.putIfAbsent("concurrency", trainConcurrency.value());

        stub.execute(executionContext.algorithmsProcedureFacade(), graphName, configCopy);
    }

    @Override
    public Map<String, Object> config() {
        return configuration;
    }

    /**
     * We happen to know that these are always mutate mode procedures
     */
    @Override
    public String procName() {
        return canonicalProcedureName.getRawForm() + ".mutate";
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
        var configCopy = new HashMap<>(configuration);
        configCopy.put("relationshipTypes", relTypes);
        configCopy.put("nodeLabels", nodeLabels);

        try {

            return stub.getMemoryEstimation(algorithmsProcedureFacade, username, configCopy);
        } catch (MemoryEstimationNotImplementedException exception) {
            // If a single node property step cannot be estimated, we ignore this step in the estimation
            return MemoryEstimations.of(canonicalProcedureName.getRawForm() + ".mutate", MemoryRange.of(0));
        }
    }

    @Override
    public String mutateNodeProperty() {
        return config().get(MUTATE_PROPERTY_KEY).toString();
    }

    @Override
    public Map<String, Object> toMap() {
        var configWithContext = new LinkedHashMap<>(configuration);
        configWithContext.put(CONTEXT_NODE_LABELS, contextNodeLabels);
        configWithContext.put(CONTEXT_RELATIONSHIP_TYPES, contextRelationshipTypes);

        return Map.of("name", procName(), "config", configWithContext);
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
    public String rootTaskName() {
        return algorithmMetadata.labelForProgressTracking;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StubPoweredNodePropertyStep that = (StubPoweredNodePropertyStep) o;
        return Objects.equals(canonicalProcedureName, that.canonicalProcedureName) &&
            Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalProcedureName, configuration);
    }
}
