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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.io.PrintWriter;

final class CypherExporter {
    static void export(PrintWriter out, GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            StringBuilder s = new StringBuilder();
            final ResourceIterable<Node> nodes = db.getAllNodes();
            nodes.forEach(node -> node(node, s).append(System.lineSeparator()));
            s.append("CREATE").append(System.lineSeparator());
            nodes.forEach(node -> node
                    .getRelationships(Direction.OUTGOING)
                    .forEach(rel -> rel(rel, s).append(',').append(System.lineSeparator())));
            s.append(System.lineSeparator());
            out.println(s.toString());
            tx.success();
        }
        out.flush();
    }

    private static StringBuilder node(Node node, StringBuilder s) {
        s.append("CREATE (n").append(node.getId());
        for (Label label : node.getLabels()) {
            s.append(':').append(label.name());
        }
        return s.append(props(node, s)).append(')');
    }

    private static StringBuilder rel(Relationship rel, StringBuilder s) {
        return s
                .append("  (n")
                .append(rel.getStartNode().getId())
                .append(")-[:")
                .append(rel.getType().name())
                .append(props(rel, s))
                .append("]->")
                .append("(n")
                .append(rel.getEndNode().getId())
                .append(')');
    }

    private static String props(PropertyContainer prop, StringBuilder s) {
        int length = s.length();
        s.append(" {");
        for (String propKey : prop.getPropertyKeys()) {
            Object propValue = prop.getProperty(propKey);
            s.append(propKey).append(':').append(propValue);
        }
        if (s.length() - 2 != length) {
            s.append('}');
        } else {
            s.setLength(length);
        }
        return "";
    }
}
