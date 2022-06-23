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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

public abstract class PipelineTrain<RESULT, CONFIG extends AlgoBaseConfig, PIPELINE extends TrainingPipeline<?>> extends Algorithm<RESULT> {
    protected final PIPELINE pipeline;
    protected final GraphStore graphStore;
    protected final CONFIG config;

    protected PipelineTrain(
        PIPELINE pipeline,
        GraphStore graphStore,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipeline = pipeline;
        this.graphStore = graphStore;
        this.config = config;
    }

    protected abstract RESULT unsafeCompute();

    @Override
    public RESULT compute() {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);
        pipeline.validateBeforeExecution(graphStore, config);
        try {
            return unsafeCompute();
        }
        finally {
            cleanUpGraphStore();
        }
    }

    protected void cleanUpGraphStore() {
        removeNodeProperties(graphStore);
    }

    private void removeNodeProperties(GraphStore graphstore) {
        pipeline.nodePropertySteps().forEach(step -> {
            var intermediateProperty = step.config().get(MUTATE_PROPERTY_KEY);
            if (intermediateProperty instanceof String) {
                graphstore.removeNodeProperty(((String) intermediateProperty));
            }
        });
    }

    @Override
    public void release() {

    }
}
