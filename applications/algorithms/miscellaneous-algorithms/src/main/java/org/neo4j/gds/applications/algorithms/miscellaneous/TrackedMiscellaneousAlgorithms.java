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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.MiscellaneousAlgorithmsTasks;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.indexInverse.InverseRelationshipsParamsTransformer;
import org.neo4j.gds.walking.CollapsePathConfig;
import org.neo4j.gds.walking.CollapsePathParamsTransformer;

import java.util.Map;

class TrackedMiscellaneousAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final MiscellaneousAlgorithms miscellaneousAlgorithms;
    private final ProgressTrackerCreator progressTrackerCreator;

    TrackedMiscellaneousAlgorithms(
        MiscellaneousAlgorithms miscellaneousAlgorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.miscellaneousAlgorithms = miscellaneousAlgorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    SingleTypeRelationships collapsePath(GraphStore graphStore, CollapsePathConfig configuration) {
        var params = CollapsePathParamsTransformer.create(configuration, graphStore);

        return miscellaneousAlgorithms.collapsePath(graphStore, params);
    }

    Map<RelationshipType, SingleTypeRelationships> indexInverse(
        NodeTranslator nodeTranslator,
        GraphStore graphStore,
        InverseRelationshipsConfig configuration
    ) {
        var params = InverseRelationshipsParamsTransformer.toParameters(graphStore, configuration);

        var task = MiscellaneousAlgorithmsTasks.inverseIndex(nodeTranslator.nodeCount(), params);
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> miscellaneousAlgorithms.indexInverse(graphStore, params, progressTracker),
            progressTracker,
            true
        );
    }
}
