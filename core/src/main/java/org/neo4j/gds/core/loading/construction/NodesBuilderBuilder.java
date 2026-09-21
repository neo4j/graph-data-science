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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Objects;
import java.util.Optional;

public final class NodesBuilderBuilder {
    private Optional<Long> maxOriginalId = Optional.empty();
    private Optional<Long> nodeCount = Optional.empty();
    private NodeSchema nodeSchema = NodeSchema.empty();
    private boolean hasLabelInformation = false;
    private boolean deduplicateIds = true;
    private Concurrency concurrency = new Concurrency(1);
    private String idMapBuilderType = IdMap.NO_TYPE;
    private boolean usePooledBuilderProvider = false;
    private Optional<GraphFactory.PropertyInfo> propertyInfo = Optional.empty();

    NodesBuilderBuilder() {}

    public NodesBuilderBuilder maxOriginalId(long maxOriginalId) {
        this.maxOriginalId = Optional.of(maxOriginalId);
        return this;
    }

    public NodesBuilderBuilder maxOriginalId(Optional<Long> maxOriginalId) {
        this.maxOriginalId = Objects.requireNonNull(maxOriginalId);
        return this;
    }

    public NodesBuilderBuilder nodeCount(long nodeCount) {
        this.nodeCount = Optional.of(nodeCount);
        return this;
    }

    public NodesBuilderBuilder nodeCount(Optional<Long> nodeCount) {
        this.nodeCount = Objects.requireNonNull(nodeCount);
        return this;
    }

    public NodesBuilderBuilder nodeSchema(NodeSchema nodeSchema) {
        this.nodeSchema = Objects.requireNonNull(nodeSchema);
        return this;
    }

    public NodesBuilderBuilder hasLabelInformation(boolean hasLabelInformation) {
        this.hasLabelInformation = hasLabelInformation;
        return this;
    }

    public NodesBuilderBuilder deduplicateIds(boolean deduplicateIds) {
        this.deduplicateIds = deduplicateIds;
        return this;
    }

    public NodesBuilderBuilder concurrency(Concurrency concurrency) {
        this.concurrency = Objects.requireNonNull(concurrency);
        return this;
    }

    public NodesBuilderBuilder idMapBuilderType(String idMapBuilderType) {
        this.idMapBuilderType = Objects.requireNonNull(idMapBuilderType);
        return this;
    }

    public NodesBuilderBuilder usePooledBuilderProvider(boolean usePooledBuilderProvider) {
        this.usePooledBuilderProvider = usePooledBuilderProvider;
        return this;
    }

    public NodesBuilderBuilder propertyInfo(GraphFactory.PropertyInfo propertyInfo) {
        this.propertyInfo = Optional.of(propertyInfo);
        return this;
    }

    public NodesBuilder build() {
        return GraphFactory.nodesBuilder(
            maxOriginalId,
            nodeCount,
            nodeSchema,
            hasLabelInformation,
            deduplicateIds,
            concurrency,
            idMapBuilderType,
            usePooledBuilderProvider,
            propertyInfo
        );
    }
}
