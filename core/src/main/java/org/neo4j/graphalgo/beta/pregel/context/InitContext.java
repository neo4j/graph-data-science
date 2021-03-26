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
package org.neo4j.graphalgo.beta.pregel.context;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.NodePropertyContainer;
import org.neo4j.graphalgo.beta.pregel.ComputeStep;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;

import java.util.Set;

/**
 * A context that is used during the initialization phase, which is before the
 * first superstep is being executed. The init context allows accessing node
 * properties from the input graph which can be used to set initial node values
 * for the Pregel computation.
 */
public final class InitContext<CONFIG extends PregelConfig> extends NodeCentricContext<CONFIG> {
    private final NodePropertyContainer nodePropertyContainer;

    public InitContext(
        ComputeStep computeStep,
        CONFIG config,
        NodePropertyContainer nodePropertyContainer
    ) {
        super(computeStep, config);
        this.nodePropertyContainer = nodePropertyContainer;
    }

    /**
     * Returns the node property keys stored in the input graph.
     * These properties can be the result of previous computations
     * or part of node projections when creating the graph.
     */
    public Set<String> nodePropertyKeys() {
        return this.nodePropertyContainer.availableNodeProperties();
    }

    /**
     * Returns the property values for the given property key.
     * Property values can be used to access individual node
     * property values by using their node identifier.
     */
    public NodeProperties nodeProperties(String key) {
        return this.nodePropertyContainer.nodeProperties(key);
    }
}
