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
package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphDimensionsReader;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.NodeImporter;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphViewFactory extends GraphFactory {

    public GraphViewFactory(
            final GraphDatabaseAPI api,
            final GraphSetup setup) {
        super(api, setup);
    }

    @Override
    public Graph importGraph() {
        GraphDimensions dimensions = new GraphDimensionsReader(api, setup).call();
        IntIdMap idMap = new NodeImporter(
                api,
                setup.tracker,
                ImportProgress.EMPTY,
                dimensions.nodeCount(),
                dimensions.labelId()
        ).call();
        return new GraphView(api, dimensions, idMap, setup.relationDefaultWeight, setup.loadAsUndirected);
    }
}
