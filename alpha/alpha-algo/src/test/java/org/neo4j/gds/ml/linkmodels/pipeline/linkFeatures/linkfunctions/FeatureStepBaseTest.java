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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

@GdlExtension
public class FeatureStepBaseTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String GRAPH = "(a:N {noise: 42, z: 13, array: [3.0,2.0], zeros: [.0, .0], invalidValue: NaN}), " +
                          "(b:N {noise: 1337, z: 0, array: [1.0,1.0], zeros: [.0, .0], invalidValue: 1.0}), " +
                          "(c:N {noise: 42, z: 2, array: [8.0,2.3], zeros: [.0, .0], invalidValue: 3.0}), " +
                          "(d:N {noise: 42, z: 9, array: [0.1,91.0], zeros: [.0, .0], invalidValue: 4.0}), " +

                          "(a)-[:REL]->(b), " +
                          "(a)-[:REL]->(c), " +
                          "(a)-[:REL]->(d)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;
}
