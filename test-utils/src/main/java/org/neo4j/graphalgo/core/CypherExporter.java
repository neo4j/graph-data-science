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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Exports a graph as Cypher statements, so that you could inspect it during tests or similar.
 * Absolutely NOT intented for any kind of larger graph (say, > 100 nodes).
 *
 * The Cypher statement should be valid, but might not reproduce the input graph 100%.
 */
public final class CypherExporter {

    /**
     * Writes a Cypher statement describing the graph to the given writer.
     */
    public static void export(PrintWriter out, GraphDatabaseService db) {
        export(out, db, NeoData.INSTANCE);
    }

    /**
     * Writes a Cypher statement describing the graph to the given writer.
     * Information about labels and relationship types is lost and won't be reproduced.
     * Property names and ids will reflect internal names/values and not the actual ids/values.
     */
    public static void export(PrintWriter out, Graph graph) {
        export(out, graph, GraphData.INSTANCE);
    }

    private static <G, NP, N extends NP, RP, R extends RP> void export(
            PrintWriter out,
            G graph,
            GraphLike<G, NP, N, RP, R> graphLike) {
        try {
            StringBuilder sb = new StringBuilder();
            NodeLike<N> nodeLike = graphLike.nodeLike();
            RelationshipLike<R> relLike = graphLike.relLike();
            PropertiesLike<NP, G> nodePropsLike = graphLike.nodePropsLike();
            PropertiesLike<RP, G> relPropsLike = graphLike.relPropsLike();

            graphLike.runInTx(graph, () -> {
                graphLike.forEachNode(graph, n -> {
                    node(n, graph, nodeLike, nodePropsLike, sb)
                            .append(System.lineSeparator());
                });
                sb.append("CREATE").append(System.lineSeparator());
                graphLike.forEachNode(graph, n -> {
                    graphLike.forEachOutgoing(graph, n, r -> {
                        rel(r, graph, relLike, relPropsLike, sb)
                                .append(',').append(System.lineSeparator());
                    });
                });
            });

            sb.append(System.lineSeparator());
            out.println(sb.toString());
        } finally {

            out.flush();
        }
    }

    private static <T, C> StringBuilder node(
            T item,
            C context,
            NodeLike<? super T> node,
            PropertiesLike<? super T, C> props,
            StringBuilder s) {
        s.append("CREATE (n").append(node.id(item));
        for (String label : node.labels(item)) {
            s.append(':').append(label);
        }
        return s.append(props(item, context, props, s)).append(')');
    }

    private static <T, C> StringBuilder rel(
            T item,
            C context,
            RelationshipLike<? super T> rel,
            PropertiesLike<? super T, C> props,
            StringBuilder s) {
        s.append("  (n").append(rel.startId(item)).append(")-[");
        rel.type(item).ifPresent(type -> s.append(":").append(type));
        return s.append(props(item, context, props, s))
                .append("]->")
                .append("(n")
                .append(rel.endId(item))
                .append(')');
    }

    private static <T, C> String props(
            T item,
            C context,
            PropertiesLike<T, C> props,
            StringBuilder s) {
        int length = s.length();
        s.append(" {");
        for (String propKey : props.availableKeys(item, context)) {
            Object propValue = props.property(propKey, item, context);
            s.append(propKey).append(':').append(propValue);
        }
        if (s.length() - 2 != length) {
            s.append('}');
        } else {
            s.setLength(length);
        }
        return "";
    }

    interface GraphLike<G, NP, N extends NP, RP, R extends RP> {
        NodeLike<N> nodeLike();

        RelationshipLike<R> relLike();

        PropertiesLike<NP, G> nodePropsLike();

        PropertiesLike<RP, G> relPropsLike();

        void runInTx(G graph, Runnable action);

        void forEachNode(G graph, Consumer<N> action);

        void forEachOutgoing(G graph, N node, Consumer<R> action);
    }

    interface NodeLike<T> {
        long id(T t);

        Iterable<String> labels(T t);
    }

    interface RelationshipLike<T> {
        long startId(T t);

        long endId(T t);

        Optional<String> type(T t);
    }

    interface PropertiesLike<T, Context> {
        Iterable<String> availableKeys(T t, Context context);

        Object property(String key, T t, Context context);
    }

    enum NeoData implements GraphLike<GraphDatabaseService, PropertyContainer, Node, PropertyContainer, Relationship>, NodeLike<Node>, RelationshipLike<Relationship>, PropertiesLike<PropertyContainer, GraphDatabaseService> {
        INSTANCE;

        @Override
        public void runInTx(GraphDatabaseService graph, Runnable action) {
            try (Transaction tx = graph.beginTx()) {
                action.run();
                tx.success();
            }
        }


        @Override
        public void forEachNode(GraphDatabaseService graph, Consumer<Node> action) {
            graph.getAllNodes().forEach(action);
        }

        @Override
        public void forEachOutgoing(
                GraphDatabaseService graph,
                Node node,
                Consumer<Relationship> action) {
            node.getRelationships(Direction.OUTGOING).forEach(action);
        }


        @Override
        public long id(Node node) {
            return node.getId();
        }

        @Override
        public Iterable<String> labels(Node node) {
            return Iterables.stream(node.getLabels())
                    .map(Label::name)
                    .collect(Collectors.toList());
        }

        @Override
        public long startId(Relationship relationship) {
            return relationship.getStartNodeId();
        }

        @Override
        public long endId(Relationship relationship) {
            return relationship.getEndNodeId();
        }

        @Override
        public Optional<String> type(Relationship relationship) {
            return Optional.of(relationship.getType().name());
        }


        @Override
        public Iterable<String> availableKeys(PropertyContainer propertyContainer, GraphDatabaseService context) {
            return propertyContainer.getPropertyKeys();
        }

        @Override
        public Object property(String key, PropertyContainer propertyContainer, GraphDatabaseService context) {
            return propertyContainer.getProperty(key);
        }

        @Override
        public NodeLike<Node> nodeLike() {
            return this;
        }

        @Override
        public RelationshipLike<Relationship> relLike() {
            return this;
        }

        @Override
        public PropertiesLike<PropertyContainer, GraphDatabaseService> nodePropsLike() {
            return this;
        }

        @Override
        public PropertiesLike<PropertyContainer, GraphDatabaseService> relPropsLike() {
            return this;
        }
    }

    enum GraphData implements GraphLike<Graph, Long, Long, Pair<Long, Long>, Pair<Long, Long>> {
        INSTANCE;

        @Override
        public NodeLike<Long> nodeLike() {
            return GraphNode.INSTANCE;
        }

        @Override
        public RelationshipLike<Pair<Long, Long>> relLike() {
            return GraphRel.INSTANCE;
        }

        @Override
        public PropertiesLike<Long, Graph> nodePropsLike() {
            return GraphNode.INSTANCE;
        }

        @Override
        public PropertiesLike<Pair<Long, Long>, Graph> relPropsLike() {
            return GraphRel.INSTANCE;
        }

        @Override
        public void runInTx(Graph graph, Runnable action) {
            action.run();
        }

        @Override
        public void forEachNode(Graph graph, Consumer<Long> action) {
            graph.forEachNode(node -> {
                action.accept(node);
                return true;
            });
        }

        @Override
        public void forEachOutgoing(Graph graph, Long node, Consumer<Pair<Long, Long>> action) {
            graph.forEachRelationship(node, Direction.OUTGOING, (s, t) -> {
                action.accept(Pair.of(s, t));
                return true;
            });
        }
    }

    enum GraphNode implements NodeLike<Long>, PropertiesLike<Long, Graph> {
        INSTANCE;

        @Override
        public long id(Long nodeId) {
            return nodeId;
        }

        @Override
        public Iterable<String> labels(Long aLong) {
            return Collections.emptyList();
        }

        @Override
        public Iterable<String> availableKeys(Long nodeId, Graph graph) {
            return graph.availableNodeProperties();
        }

        @Override
        public Object property(String key, Long nodeId, Graph graph) {
            return graph.nodeProperties(key).nodeWeight(nodeId);
        }
    }

    enum GraphRel implements RelationshipLike<Pair<Long, Long>>, PropertiesLike<Pair<Long, Long>, Graph> {
        INSTANCE;

        @Override
        public long startId(Pair<Long, Long> startAndEndNodeId) {
            return startAndEndNodeId.first();
        }

        @Override
        public long endId(Pair<Long, Long> startAndEndNodeId) {
            return startAndEndNodeId.other();
        }

        @Override
        public Optional<String> type(Pair<Long, Long> startAndEndNodeId) {
            return Optional.empty();
        }

        @Override
        public Iterable<String> availableKeys(Pair<Long, Long> startAndEndNodeId, Graph graph) {
            return Collections.singleton("weight");
        }

        @Override
        public Object property(String key, Pair<Long, Long> startAndEndNodeId, Graph graph) {
            return graph.weightOf(startAndEndNodeId.first(), startAndEndNodeId.other());
        }
    }

    private CypherExporter() {}
}
