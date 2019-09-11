/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class CypherGraphFactory extends GraphFactory {

    public static final String TYPE = "cypher";

    static final String LIMIT = "limit";
    static final String SKIP = "skip";

    private final GraphDatabaseAPI api;
    private final GraphSetup setup;

    public CypherGraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        super(api, setup);
        this.api = api;
        this.setup = setup;
    }

    @Override
    protected void validateTokens() { }

    public final MemoryEstimation memoryEstimation() {
        BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.startLabel, api, setup).load();
        dimensions.nodeCount(nodeCount.rows());

        BatchLoadResult relCount = new CountingCypherRecordLoader(setup.relationshipType, api, setup).load();
        dimensions.maxRelCount(relCount.rows());

        return HeavyGraphFactory.getMemoryEstimation(setup, dimensions);
    }

    @Override
    public Graph importGraph() {
        BatchLoadResult nodeCount = new CountingCypherRecordLoader(setup.startLabel, api, setup).load();
        IdsAndProperties nodes = new CypherNodeLoader(nodeCount.rows(), api, setup).load();
        Relationships relationships = new CypherRelationshipLoader(nodes.idMap(), api, setup).load();

        return new HugeGraph(
                setup.tracker,
                nodes.idMap(),
                nodes.properties(),
                relationships.relationshipCount(),
                relationships.inAdjacency(),
                relationships.outAdjacency(),
                relationships.inOffsets(),
                relationships.outOffsets(),
                relationships.defaultWeight(),
                relationships.inWeights(),
                relationships.outWeights(),
                relationships.inWeightOffsets(),
                relationships.outWeightOffsets(),
                setup.loadAsUndirected);
    }
}
