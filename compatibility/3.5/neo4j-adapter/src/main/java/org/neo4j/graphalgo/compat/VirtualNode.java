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
package org.neo4j.graphalgo.compat;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;

public class VirtualNode implements Node {
    private static AtomicLong MIN_ID = new AtomicLong(-1);
    private final List<Label> labels = new ArrayList<>();
    private final Map<String, Object> props = new HashMap<>();
    private final List<Relationship> rels = new ArrayList<>();
    private final GraphDatabaseService db;
    private final long id;

    // need to keep db param to maintain compat with 3.5 API
    @SuppressWarnings("unused")
    public VirtualNode(Label[] labels, Map<String, Object> props, GraphDatabaseService db) {
        this.id = MIN_ID.getAndDecrement();
        this.db = db;
        this.labels.addAll(asList(labels));
        this.props.putAll(props);
    }

    // need to keep db param to maintain compat with 3.5 API
    @SuppressWarnings("unused")
    public VirtualNode(long nodeId, Label[] labels, Map<String, Object> props, GraphDatabaseService db) {
        this.id = nodeId;
        this.db = db;
        this.labels.addAll(asList(labels));
        this.props.putAll(props);
    }

    // need to keep db param to maintain compat with 3.5 API
    @SuppressWarnings("unused")
    public VirtualNode(long nodeId, GraphDatabaseService db) {
        this.id = nodeId;
        this.db = db;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public void delete() {
        for (Relationship rel : rels) {
            rel.delete();
        }
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return rels;
    }

    @Override
    public boolean hasRelationship() {
        return !rels.isEmpty();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... relationshipTypes) {
        return new FilteringIterable<>(rels, (r) -> isType(r, relationshipTypes));
    }

    private boolean isType(Relationship r, RelationshipType... relationshipTypes) {
        for (RelationshipType type : relationshipTypes) {
            if (r.isType(type)) return true;
        }
        return false;
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... relationshipTypes) {
        return new FilteringIterable<>(rels, (r) -> isType(r, relationshipTypes) && isDirection(r, direction));
    }

    private boolean isDirection(Relationship r, Direction direction) {
        return direction == Direction.BOTH || direction == Direction.OUTGOING && r.getStartNode().equals(this) || direction == Direction.INCOMING && r.getEndNode().equals(this);
    }

    @Override
    public boolean hasRelationship(RelationshipType... relationshipTypes) {
        return getRelationships(relationshipTypes).iterator().hasNext();
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... relationshipTypes) {
        return getRelationships(direction, relationshipTypes).iterator().hasNext();
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction) {
        return new FilteringIterable<>(rels, (r) -> isDirection(r, direction));
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        return getRelationships(direction).iterator().hasNext();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType relationshipType, Direction direction) {
        return new FilteringIterable<>(rels, (r) -> isType(r, relationshipType) && isDirection(r, direction));
    }

    @Override
    public boolean hasRelationship(RelationshipType relationshipType, Direction direction) {
        return getRelationships(relationshipType, direction).iterator().hasNext();
    }
    @Override
    public Relationship getSingleRelationship(RelationshipType relationshipType, Direction direction) {
        Relationship relationship = null;
        Iterator<Relationship> iterator = getRelationships(direction, relationshipType).iterator();
        if (iterator.hasNext()) {
            relationship = iterator.next();
            if (iterator.hasNext()) {
                throw new IllegalStateException("There is more than one relationship.");
            }
        }
        return relationship;
    }

    @Override
    public VirtualRelationship createRelationshipTo(Node node, RelationshipType relationshipType) {
        VirtualRelationship rel = new VirtualRelationship(this, node, relationshipType);
        rels.add(rel);
        return rel;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return rels.stream().map(Relationship::getType).collect(Collectors.toList());
    }

    @Override
    public GraphDatabaseService getGraphDatabase() {
        return db;
    }

    @Override
    public int getDegree() {
        return rels.size();
    }

    @Override
    public int getDegree(RelationshipType relationshipType) {
        return (int) StreamSupport.stream(getRelationships(relationshipType).spliterator(), false).count();
    }

    @Override
    public int getDegree(Direction direction) {
        return (int) StreamSupport.stream(getRelationships(direction).spliterator(), false).count();
    }

    @Override
    public int getDegree(RelationshipType relationshipType, Direction direction) {
        return (int) StreamSupport.stream(getRelationships(direction, relationshipType).spliterator(), false).count();
    }

    @Override
    public void addLabel(Label label) {
        labels.add(label);
    }

    @Override
    public void removeLabel(Label label) {
        labels.removeIf(next -> next.name().equals(label.name()));
    }

    @Override
    public boolean hasLabel(Label label) {
        for (Label l : labels) {
            if (l.name().equals(label.name())) return true;
        }
        return false;
    }

    @Override
    public Iterable<Label> getLabels() {
        return labels;
    }

    @Override
    public boolean hasProperty(String s) {
        return props.containsKey(s);
    }

    @Override
    public Object getProperty(String s) {
        return props.get(s);
    }

    @Override
    public Object getProperty(String s, Object o) {
        Object value = props.get(s);
        return value == null ? o : value;
    }

    @Override
    public void setProperty(String s, Object o) {
        props.put(s,o);
    }

    @Override
    public Object removeProperty(String s) {
        return props.remove(s);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return props.keySet();
    }

    @Override
    public Map<String, Object> getProperties(String... strings) {
        HashMap<String, Object> res = new HashMap<>(props);
        res.keySet().retainAll(asList(strings));
        return res;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return props;
    }

    void delete(Relationship rel) {
        rels.remove(rel);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Node && id == ((Node) o).getId();

    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString()
    {
        return "VirtualNode{" + "labels=" + labels + ", props=" + props + ", rels=" + rels + '}';
    }

    /**
     * An iterator which filters another iterator, only letting items with certain
     * criteria pass through. All iteration/filtering is done lazily.
     *
     * @param <T> the type of items in the iteration.
     */
    static class FilteringIterator<T> implements Iterator<T> {
        private final Iterator<T> source;
        private final Predicate<T> predicate;
        private T nextElement;

        public FilteringIterator(Iterator<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public boolean hasNext() {
            if (nextElement == null) {
                while (source.hasNext()) {
                    T testItem = source.next();
                    if (predicate.test(testItem)) {
                        nextElement = testItem;
                        break;
                    }
                }
            }
            return nextElement != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return nextElement;
            } finally {
                nextElement = null;
            }
        }
    }


    /**
     * An iterable which filters another iterable, only letting items with certain
     * criteria pass through. All iteration/filtering is done lazily.
     *
     * @param <T> the type of items in the iteration.
     */
    static class FilteringIterable<T> implements Iterable<T> {
        private final Iterable<T> source;
        private final Predicate<T> predicate;

        public FilteringIterable(Iterable<T> source, Predicate<T> predicate) {
            this.source = source;
            this.predicate = predicate;
        }

        @Override
        public Iterator<T> iterator() {
            return new FilteringIterator<>(source.iterator(), predicate);
        }
    }

}
