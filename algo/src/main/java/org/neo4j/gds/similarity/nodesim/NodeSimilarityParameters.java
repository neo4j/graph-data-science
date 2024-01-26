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
package org.neo4j.gds.similarity.nodesim;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Parameters;

@Parameters
public final class NodeSimilarityParameters {

    public static NodeSimilarityParameters create(
        int concurrency,
        MetricSimilarityComputer similarityComputer,
        int degreeCutoff,
        int upperDegreeCutoff,
        int normalizedK,
        int normalizedN,
        boolean computeToStream,
        boolean hasRelationshipWeightProperty,
        boolean useComponents,
        @Nullable String componentProperty
    ) {
        return new NodeSimilarityParameters(
            concurrency,
            similarityComputer,
            degreeCutoff,
            upperDegreeCutoff,
            normalizedK,
            normalizedN,
            computeToStream,
            hasRelationshipWeightProperty,
            useComponents,
            componentProperty
        );
    }

    private final int concurrency;
    private final MetricSimilarityComputer similarityComputer;
    private final int degreeCutoff;
    private final int upperDegreeCutoff;
    private final int normalizedK;
    private final int normalizedN;
    private final boolean computeToStream;
    private final boolean hasRelationshipWeightProperty;
    private final boolean useComponents;
    private final String componentProperty;

    private NodeSimilarityParameters(
        int concurrency,
        MetricSimilarityComputer similarityComputer,
        int degreeCutoff,
        int upperDegreeCutoff,
        int normalizedK,
        int normalizedN,
        boolean computeToStream,
        boolean hasRelationshipWeightProperty,
        boolean useComponents,
        @Nullable String componentProperty
    ) {
        this.concurrency = concurrency;
        this.similarityComputer = similarityComputer;
        this.degreeCutoff = degreeCutoff;
        this.upperDegreeCutoff = upperDegreeCutoff;
        this.normalizedK = normalizedK;
        this.normalizedN = normalizedN;
        this.computeToStream = computeToStream;
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
        this.useComponents = useComponents;
        this.componentProperty = componentProperty;
    }

    public int concurrency() {
        return concurrency;
    }

    public MetricSimilarityComputer similarityComputer() {
        return similarityComputer;
    }

    int degreeCutoff() {
        return degreeCutoff;
    }

    int upperDegreeCutoff() {
        return upperDegreeCutoff;
    }

    public int normalizedK() {
        return normalizedK;
    }

    public int normalizedN() {
        return normalizedN;
    }

    public boolean computeToStream() {
        return computeToStream;
    }

    boolean hasRelationshipWeightProperty() {
        return hasRelationshipWeightProperty;
    }

    boolean hasTopK() {
        return normalizedK != 0;
    }

    boolean hasTopN() {
        return normalizedN != 0;
    }

    boolean isParallel() {
        return concurrency > 1;
    }

    // WCC specialization

    public boolean useComponents() {
        return useComponents;
    }

    @Nullable
    public String componentProperty() {
        return componentProperty;
    }

    boolean runWCC() {
        return useComponents && componentProperty == null;
    }
}
