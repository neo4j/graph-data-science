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
package org.neo4j.gds.model.storage;

import com.google.protobuf.GeneratedMessageV3;
import org.neo4j.gds.TrainConfigSerializer;
import org.neo4j.gds.config.GraphSageTrainConfigSerializer;
import org.neo4j.gds.config.LinkPredictionTrainConfigSerializer;
import org.neo4j.gds.config.NodeClassificationTrainConfigSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.ml.linkmodels.LinkPredictionTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.utils.StringJoining;

import static org.neo4j.gds.model.ModelSupport.SUPPORTED_TYPES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TrainConfigSerializerFactory {

    private TrainConfigSerializerFactory() {}

    public static <TC extends ModelConfig, PD extends GeneratedMessageV3, R extends TrainConfigSerializer<TC, PD>> R trainConfigSerializer(String algoType) {
        switch (algoType) {
            case GraphSage.MODEL_TYPE:
                return (R) new GraphSageTrainConfigSerializer();
            case NodeClassificationTrain.MODEL_TYPE:
                return (R) new NodeClassificationTrainConfigSerializer();
            case LinkPredictionTrain.MODEL_TYPE:
                return (R) new LinkPredictionTrainConfigSerializer();
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "Unknown model type '%s', supported model types are: %s.",
                    algoType,
                    StringJoining.join(SUPPORTED_TYPES)
                ));
        }
    }
}
