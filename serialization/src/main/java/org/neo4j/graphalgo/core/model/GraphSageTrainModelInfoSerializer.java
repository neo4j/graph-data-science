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
package org.neo4j.graphalgo.core.model;

import com.google.protobuf.GeneratedMessageV3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.neo4j.gds.ModelInfoSerializer;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ImmutableGraphSageTrainMetrics;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class GraphSageTrainModelInfoSerializer implements ModelInfoSerializer {

    public GeneratedMessageV3 toSerializable(Model.Mappable mappable) {
        var modelInfo = (GraphSageModelTrainer.GraphSageTrainMetrics) mappable;
        return GraphSageProto.GraphSageMetrics.newBuilder()
            .addAllEpochLosses(modelInfo.epochLosses())
            .setDidConverge(modelInfo.didConverge())
            .build();
    }

    public Model.Mappable fromSerializable(GeneratedMessageV3 generatedMessageV3) {
        var protoModelInfo = (GraphSageProto.GraphSageMetrics) generatedMessageV3;
        return ImmutableGraphSageTrainMetrics.builder()
            .didConverge(protoModelInfo.getDidConverge())
            .epochLosses(protoModelInfo.getEpochLossesList())
            .build();
    }

    @Override
    public Class<GraphSageProto.GraphSageMetrics> serializableClass() {
        return GraphSageProto.GraphSageMetrics.class;
    }
}
