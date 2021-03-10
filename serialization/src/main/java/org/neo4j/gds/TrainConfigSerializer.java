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
package org.neo4j.gds;

import com.google.protobuf.Any;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;

public interface TrainConfigSerializer<TRAIN_CONFIG, PROTO_TRAIN_CONFIG extends GeneratedMessageV3> {
    PROTO_TRAIN_CONFIG toSerializable(TRAIN_CONFIG trainConfig);
    TRAIN_CONFIG fromSerializable(PROTO_TRAIN_CONFIG protoTrainConfig);
    default TRAIN_CONFIG fromSerializable(Any protoTrainConfig) {
        try {
            var unpacked = protoTrainConfig.unpack(serializableClass());
            return fromSerializable(unpacked);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    Class<PROTO_TRAIN_CONFIG> serializableClass();
}
