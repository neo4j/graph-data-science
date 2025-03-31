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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.collapsepath.CollapsePathParameters;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesParameters;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.undirected.ToUndirected;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.walking.CollapsePath;

import java.util.Map;

public class MiscellaneousAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public MiscellaneousAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    public SingleTypeRelationships collapsePath(GraphStore graphStore, CollapsePathParameters parameters) {

        var algorithm = CollapsePath.create(
                graphStore,
                parameters,
                DefaultPool.INSTANCE
        );

        return algorithm.compute();
    }

    Map<RelationshipType, SingleTypeRelationships> indexInverse(
        GraphStore graphStore,
        InverseRelationshipsParameters parameters,
        ProgressTracker progressTracker
    ) {

        var algorithm = new InverseRelationships(
            graphStore,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }


    public ScalePropertiesResult scaleProperties(
        Graph graph,
        ScalePropertiesParameters params,
        ProgressTracker progressTracker
    ) {
        var algorithm = new ScaleProperties(
            graph,
            params,
            progressTracker,
            DefaultPool.INSTANCE
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            params.concurrency()
        );
    }

    public SingleTypeRelationships toUndirected(GraphStore graphStore, ToUndirectedConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.ToUndirected.asString(),
            Tasks.leaf("Create Undirected Relationships", graphStore.nodeCount()),
            Tasks.leaf("Build undirected Adjacency list")
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );

        var algorithm = new ToUndirected(
            graphStore,
            configuration,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }
}
