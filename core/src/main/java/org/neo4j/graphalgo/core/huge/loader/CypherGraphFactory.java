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

/**
 * @author mknblch
 */
public class CypherGraphFactory extends GraphFactory {

    static final int NO_BATCH = -1;
    static final String LIMIT = "limit";
    static final String SKIP = "skip";
    public static final String TYPE = "cypher";
    private final CypherNodeCountingLoader nodeCountingLoader;
    private final CypherRelationshipCountingLoader relationshipCountingLoader;
    private final CypherNodeLoader nodeLoader;
    private final CypherRelationshipLoader relationshipLoader;

    public CypherGraphFactory(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api, setup);
        this.nodeLoader = new CypherNodeLoader(api, setup);
        this.relationshipLoader = new CypherRelationshipLoader(api, setup);
        this.nodeCountingLoader = new CypherNodeCountingLoader(api, setup);
        this.relationshipCountingLoader = new CypherRelationshipCountingLoader(api, setup);
    }

    @Override
    protected void validateTokens() { }

    public final MemoryEstimation memoryEstimation() {
        ImportState nodeCount = nodeCountingLoader.load();
        dimensions.nodeCount(nodeCount.rows());

        ImportState relCount = relationshipCountingLoader.load();
        dimensions.maxRelCount(relCount.rows());

        return HeavyGraphFactory.getMemoryEstimation(setup, dimensions);
    }

    @Override
    public Graph importGraph() {
        ImportState nodeCount = nodeCountingLoader.load();
        IdsAndProperties nodes = nodeLoader.load(nodeCount.rows());
        Relationships relationships = relationshipLoader.load(nodes);

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
                relationships.outWeightOffsets());
    }
}
