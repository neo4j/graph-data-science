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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArraySerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class BitIdMapSerializer extends Serializer<BitIdMap> {

    private BitIdMapSerializer() {}

    public static void serialize(BitIdMap bitIdMap, Path exportPath) throws FileNotFoundException {
        var kryo = new Kryo();
        kryo.setRegistrationRequired(false);

        try (var output = new Output(new FileOutputStream(exportPath.resolve("id_map.bin").toFile()))) {
            kryo.writeObject(output, bitIdMap, new BitIdMapSerializer());
        }
    }

    public static BitIdMap deserialize(Path importPath) throws FileNotFoundException {
        var kryo = new Kryo();
        kryo.setRegistrationRequired(false);

        try (var input = new Input(new FileInputStream(importPath.resolve("id_map.bin").toFile()))) {
            return kryo.readObject(input, BitIdMap.class, new BitIdMapSerializer());
        }
    }

    @Override
    public void write(Kryo kryo, Output output, BitIdMap idMap) {
        kryo.register(NodeLabel.class, new NodeLabelSerializer());

        kryo.writeObject(output, idMap.labelInformation());

        kryo.writeObject(output, idMap.sparseLongArray(), new SparseLongArraySerializer());
    }

    @Override
    public BitIdMap read(Kryo kryo, Input input, Class<? extends BitIdMap> type) {
        kryo.register(NodeLabel.class, new NodeLabelSerializer());

        Map<NodeLabel, BitSet> labelInformation = kryo.readObject(input, HashMap.class);
        SparseLongArray sparseLongArray = kryo.readObject(
            input,
            SparseLongArray.class,
            new SparseLongArraySerializer()
        );
        return new BitIdMap(sparseLongArray, labelInformation, AllocationTracker.create());
    }

    static class NodeLabelSerializer extends Serializer<NodeLabel> {
        @Override
        public void write(Kryo kryo, Output output, NodeLabel nodeLabel) {
            kryo.writeObject(output, nodeLabel.name());
        }

        @Override
        public NodeLabel read(Kryo kryo, Input input, Class<? extends NodeLabel> type) {
            return NodeLabel.of(kryo.readObject(input, String.class));
        }
    }
}
