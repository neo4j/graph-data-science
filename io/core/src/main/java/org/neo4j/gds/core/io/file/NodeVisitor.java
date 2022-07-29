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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.id.IdSequence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class NodeVisitor extends ElementVisitor<NodeSchema, NodeLabel, PropertySchema> {

    private static final List<String> EMPTY_LABELS = Collections.emptyList();
    private static final Set<NodeLabel> EMPTY_LABELS_LABEL = Set.of(NodeLabel.ALL_NODES);
    private long currentId;
    private List<String> currentLabels;
    private String labelIdentifier;

    protected NodeVisitor(NodeSchema nodeSchema) {
        super(nodeSchema);
        reset();
    }

    // Accessors for node related data

    public long id() {
        return currentId;
    }

    public List<String> labels() {
        return currentLabels;
    }

    // Additional listeners for node related data

    @Override
    public boolean id(long id) {
        currentId = id;
        return true;
    }

    @Override
    public boolean id(Object id, Group group) {
        return id((long) id);
    }

    @Override
    public boolean id(Object id, Group group, IdSequence idSequence) {
        return id((long) id);
    }

    @Override
    public boolean labels(String[] labels) {
        Arrays.sort(labels);
        var labelCandidates = Arrays.asList(labels);

        if(!currentLabels.equals(labelCandidates)) {
            currentLabels = labelCandidates;
            labelIdentifier = String.join("_", labels);
        }

        return true;
    }

    // Overrides from ElementVisitor

    @Override
    protected String elementIdentifier() {
        return labelIdentifier;
    }

    @Override
    protected List<PropertySchema> getPropertySchema() {
        var nodeLabelList = currentLabels.isEmpty()
            ? EMPTY_LABELS_LABEL
            : currentLabels.stream().map(NodeLabel::of).collect(Collectors.toSet());
        var propertySchemaForLabels = elementSchema.filter(nodeLabelList);
        return new ArrayList<>(propertySchemaForLabels.unionProperties().values());
    }

    @Override
    void reset() {
        currentId = -1;
        currentLabels = EMPTY_LABELS;
        labelIdentifier = "";
    }

    abstract static class Builder<SELF extends Builder<SELF, VISITOR>, VISITOR extends NodeVisitor> {
        NodeSchema nodeSchema;

        SELF withNodeSchema(NodeSchema nodeSchema) {
            this.nodeSchema = nodeSchema;
            return me();
        }

        abstract SELF me();

        abstract VISITOR build();
    }
}
