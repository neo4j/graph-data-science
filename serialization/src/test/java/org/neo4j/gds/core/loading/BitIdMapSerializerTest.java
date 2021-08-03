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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.BitSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.SparseLongArray;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;


class BitIdMapSerializerTest extends BaseTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldSerializeBitIdMap() throws FileNotFoundException {
        long nodeCount = 10L;
        SparseLongArray.Builder builder = SparseLongArray.builder(nodeCount);
        builder.set(0, LongStream.range(0, nodeCount).toArray());
        SparseLongArray sparseLongArray = builder.build();

        Map<NodeLabel, BitSet> labelInformation = new HashMap<>();
        var fooBitSet = new BitSet(nodeCount);
        fooBitSet.set(0);
        labelInformation.put(NodeLabel.of("Foo"), fooBitSet);
        var barBitSet = new BitSet(nodeCount);
        barBitSet.set(1, nodeCount);
        labelInformation.put(NodeLabel.of("Bar"), barBitSet);

        BitIdMap bitIdMap = new BitIdMap(sparseLongArray, labelInformation, AllocationTracker.empty());
        BitIdMapSerializer.serialize(bitIdMap, tempDir);

        var deserializedIdMap = BitIdMapSerializer.deserialize(tempDir);

        assertThat(deserializedIdMap)
            .usingRecursiveComparison()
            .ignoringFields("tracker")
            .isEqualTo(bitIdMap);
    }

    @Test
    void shouldSerializeLabelInformation() throws FileNotFoundException {
        var kryo = new Kryo();
        kryo.register(NodeLabel.class, new BitIdMapSerializer.NodeLabelSerializer());
        kryo.setRegistrationRequired(false);

        Map<NodeLabel, BitSet> labelInformation = new HashMap<>();
        var fooLabelBitSet = new BitSet(42);
        fooLabelBitSet.set(10, 20);
        labelInformation.put(NodeLabel.of("Foo"), fooLabelBitSet);
        var barLabelBitSet = new BitSet(1337);
        barLabelBitSet.set(1);
        labelInformation.put(NodeLabel.of("Bar"), barLabelBitSet);

        try (var output = new Output(new FileOutputStream(tempDir.resolve("label_information.bin").toFile()))) {
            kryo.writeObject(output, labelInformation);
        }

        Map<NodeLabel, BitSet> deserializedLabelInformation;
        try (var input = new Input(new FileInputStream(tempDir.resolve("label_information.bin").toFile()))) {
            deserializedLabelInformation = kryo.readObject(input, HashMap.class);
        }

        assertThat(deserializedLabelInformation).isEqualTo(labelInformation);
    }
}
