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
package org.neo4j.gds.executor;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.Collection;
import java.util.Optional;

public class ProcedureGraphCreation<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> implements GraphCreation<ALGO, ALGO_RESULT, CONFIG> {

    private final GraphStoreLoader graphStoreLoader;
    private final MemoryUsageValidator memoryUsageValidator;
    private final CONFIG config;

    ProcedureGraphCreation(
        GraphStoreLoader graphStoreLoader,
        MemoryUsageValidator memoryUsageValidator,
        CONFIG config
    ) {
        this.graphStoreLoader = graphStoreLoader;
        this.memoryUsageValidator = memoryUsageValidator;
        this.config = config;
    }

    @Override
    public GraphStore graphStore() {
        return graphStoreLoader.graphStore();
    }

    @Override
    public Graph createGraph(GraphStore graphStore) {
        Optional<String> weightProperty = config instanceof RelationshipWeightConfig
            ? ((RelationshipWeightConfig) config).relationshipWeightProperty()
            : Optional.empty();

        Collection<NodeLabel> nodeLabels = config.nodeLabelIdentifiers(graphStore);
        Collection<RelationshipType> relationshipTypes = config.internalRelationshipTypes(graphStore);

        return graphStore.getGraph(nodeLabels, relationshipTypes, weightProperty);
    }

    @Override
    public GraphProjectConfig graphProjectConfig() {
        return graphStoreLoader.graphProjectConfig();
    }

    @Override
    public MemoryRange validateMemoryEstimation(AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory) {
        var procedureMemoryEstimation = new ProcedureMemoryEstimation<>(
            graphStoreLoader.graphDimensions(),
            algorithmFactory
        );
        return memoryUsageValidator.tryValidateMemoryUsage(config, procedureMemoryEstimation::memoryEstimation);
    }
}
