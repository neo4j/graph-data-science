/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mknblch
 */
public class HeavyCypherGraphFactory extends GraphFactory {

    static final int NO_BATCH = -1;
    static final int INITIAL_NODE_COUNT = 1_000_000;
    static final int ESTIMATED_DEGREE = 3;
    static final String LIMIT = "limit";
    static final String SKIP = "skip";
    public static final String TYPE = "cypher";
    private CypherNodeLoader nodeLoader;
    private CypherRelationshipLoader relationshipLoader;

    public HeavyCypherGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
        this.nodeLoader = new CypherNodeLoader(api, setup, dimensions);
        this.relationshipLoader = new CypherRelationshipLoader(api, setup);
    }

    public Graph build() {
        Nodes nodes = nodeLoader.load();
        Relationships relationships = relationshipLoader.load(nodes);

        if (setup.sort) {
            relationships.matrix().sortAll(setup.executor, setup.concurrency);
        }

        Map<String, WeightMapping> nodePropertyMappings = new HashMap<>();
        for (Map.Entry<PropertyMapping, WeightMap> entry : nodes.nodeProperties().entrySet()) {
            nodePropertyMappings.put(entry.getKey().propertyName, entry.getValue());
        }

        return new HeavyGraph(
                nodes.idMap,
                relationships.matrix(),
                relationships.weights(),
                nodePropertyMappings);
    }
}
