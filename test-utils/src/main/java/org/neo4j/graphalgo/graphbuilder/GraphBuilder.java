/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.graphbuilder;

import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.Transactions;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.Random;
import java.util.function.Consumer;

/**
 * The RelationshipsBuilder intends to ease the creation
 * of test graphs with well known properties
 */
public abstract class GraphBuilder<ME extends GraphBuilder<ME>> implements AutoCloseable {

    private final ME self;

    protected final HashSet<Node> nodes;
    protected final HashSet<Relationship> relationships;

    private final GraphDatabaseAPI api;
    private final Transaction tx;
    private final Random random;

    protected Label label;
    protected RelationshipType relationship;

    protected GraphBuilder(GraphDatabaseAPI api, Transaction tx, Label label, RelationshipType relationship, Random random) {
        this.api = api;
        this.tx = tx;
        this.label = label;
        this.relationship = relationship;
        nodes = new HashSet<>();
        relationships = new HashSet<>();
        this.self = me();
        this.random = random;
    }

    /**
     * set the label for all subsequent {@link GraphBuilder#createNode()} operations
     * in the current and in derived builders.
     *
     * @param label the label
     * @return child instance to make methods of the child class accessible.
     */
    public ME setLabel(String label) {
        if (null == label) {
            return self;
        }
        this.label = Label.label(label);
        return self;
    }

    /**
     * set the relationship type for all subsequent {@link GraphBuilder#createRelationship(Node, Node)}
     * operations in the current and in derived builders.
     *
     * @param relationship the name of the relationship type
     * @return child instance to make methods of the child class accessible.
     */
    public ME setRelationship(String relationship) {
        if (null == relationship) {
            return self;
        }
        this.relationship = RelationshipType.withName(relationship);
        return self;
    }

    /**
     * create a relationship between p and q with the previously defined
     * relationship type
     *
     * @param p the source node
     * @param q the target node
     * @return the relationship object
     */
    public Relationship createRelationship(Node p, Node q) {
        final Relationship relationshipTo = p.createRelationshipTo(q, relationship);
        relationships.add(relationshipTo);
        return relationshipTo;
    }

    /**
     * create a new node and set a label if previously defined
     *
     * @return the created node
     */
    public Node createNode() {
        Node node = GraphDatabaseApiProxy.createNode(api, tx);
        if (null != label) {
            node.addLabel(label);
        }
        nodes.add(node);
        return node;
    }

    /**
     * run node consumer in tx as long as he returns true
     *
     * @param consumer the node consumer
     * @return child instance to make methods of the child class accessible.
     */
    public ME forEachNodeInTx(Consumer<Node> consumer) {
        nodes.forEach(consumer);
        return self;
    }

    public ME forEachRelInTx(Consumer<Relationship> consumer) {
        relationships.forEach(consumer);
        return self;
    }

    /**
     * create a new default builder with its own node-set but
     * inherits the current label and relationship type
     *
     * @return a new default builder
     */
    public DefaultBuilder newDefaultBuilder() {
        return new DefaultBuilder(api, tx, label, relationship, random);
    }

    /**
     * create a new ring builder with its own node-set but
     * inherits current label and relationship type.
     *
     * @return a new ring builder
     */
    public RingBuilder newRingBuilder() {
        return new RingBuilder(api, tx, label, relationship, random);
    }

    /**
     * creates a grid of nodes
     * inherits current label and relationship type.
     *
     * @return the GridBuilder
     */
    public GridBuilder newGridBuilder() {
        return new GridBuilder(api, tx, label, relationship, random);
    }

    /**
     * create a complete graph where each node is interconnected
     * inherits current label and relationship type.
     *
     * @return the CompleteGraphBuilder
     */
    public CompleteGraphBuilder newCompleteGraphBuilder() {
        return new CompleteGraphBuilder(api, tx, label, relationship, random);
    }

    protected double randomDouble() {
        return random.nextDouble();
    }

    /**
     * return child instance for method chaining from methods of the abstract parent class
     *
     * @return self (child instance)
     */
    protected abstract ME me();

    /**
     * create a new default builder
     *
     * @param api the neo4j api
     * @return a new default builder
     */
    public static DefaultBuilder create(GraphDatabaseAPI api) {
        return new DefaultBuilder(api, api.beginTx(), null, null, RNGHolder.rng);
    }

    /**
     * create a new default builder with a defined RNG
     *
     * @param api    the neo4j api
     * @param random the random number generator
     * @return a new default builder
     */
    public static DefaultBuilder create(GraphDatabaseAPI api, Random random) {
        return new DefaultBuilder(api, api.beginTx(), null, null, random);
    }

    @Override
    public void close() {
        Transactions.commit(tx);
        Transactions.close(tx);
    }

    private static final class RNGHolder {
        static final Random rng = new Random();
    }
}
