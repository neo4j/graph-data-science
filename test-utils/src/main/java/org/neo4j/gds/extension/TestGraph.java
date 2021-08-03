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
package org.neo4j.gds.extension;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.triangle.intersect.RelationshipIntersectConfig;
import org.neo4j.gds.triangle.intersect.RelationshipIntersectFactory;
import org.neo4j.gds.triangle.intersect.RelationshipIntersectFactoryLocator;
import org.neo4j.graphalgo.api.CSRGraph;
import org.neo4j.graphalgo.api.CSRGraphAdapter;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIntersect;

public class TestGraph extends CSRGraphAdapter {

    private final IdFunction idFunction;
    private final String name;

    public TestGraph(CSRGraph graph, IdFunction idFunction, String name) {
        super(graph);
        this.name = name;
        this.idFunction = idFunction;
    }

    public Graph innerGraph() {
        return graph;
    }

    public long toOriginalNodeId(String variable) {
        return idFunction.of(variable);
    }

    public long toMappedNodeId(String variable) {
        return graph.toMappedNodeId(idFunction.of(variable));
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        var bitset = BitSet.newInstance();
        forEachRelationship(nodeId, (source, target) -> {
            bitset.set(target);
            return true;
        });
        return Math.toIntExact(bitset.cardinality());
    }

    @Override
    public CSRGraph concurrentCopy() {
        return new TestGraph(graph.concurrentCopy(), idFunction, name);
    }

    @Override
    public String toString() {
        return name;
    }

    @ServiceProvider
    public static final class TestGraphIntersectFactory implements RelationshipIntersectFactory {

        @Override
        public boolean canLoad(Graph graph) {
            return graph instanceof TestGraph;
        }

        @Override
        public RelationshipIntersect load(Graph graph, RelationshipIntersectConfig config) {
            assert graph instanceof TestGraph;
            var innerGraph = ((TestGraph) graph).graph();
            return RelationshipIntersectFactoryLocator
                .lookup(innerGraph)
                .orElseThrow(UnsupportedOperationException::new)
                .load(innerGraph, config);
        }
    }
}
