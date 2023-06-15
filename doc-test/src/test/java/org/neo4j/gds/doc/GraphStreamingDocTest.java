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
package org.neo4j.gds.doc;

import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipPropertiesProc;
import org.neo4j.gds.catalog.GraphStreamRelationshipsProc;
import org.neo4j.gds.degree.DegreeCentralityMutateProc;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateProc;

import java.util.List;

final class GraphStreamingDocTest extends SingleFileDocTestBase {

    @Override
    protected List<Class<?>> procedures() {
        return List.of(
            GraphProjectProc.class,
            NodeSimilarityMutateProc.class,
            DegreeCentralityMutateProc.class,
            GraphStreamNodePropertiesProc.class,
            GraphStreamRelationshipsProc.class,
            GraphStreamRelationshipPropertiesProc.class
        );
    }

    @Override
    protected List<Class<?>> functions() {
        return List.of(
            NodePropertyFunc.class,
            AsNodeFunc.class
        );
    }

    // FIXME the setup queries are not executed per group but only at the beginning of the test
    // this is a problem as the nodes and rels graph both has nodes ... we should try to use the same graph
    @Override
    protected String adocFile() {
        return "pages/management-ops/graph-inspection/graph-streaming.adoc";
    }

}
