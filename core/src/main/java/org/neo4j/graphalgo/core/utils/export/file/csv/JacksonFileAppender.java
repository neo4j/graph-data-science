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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import com.fasterxml.jackson.core.util.BufferRecyclers;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.impl.CsvEncoder;
import com.fasterxml.jackson.dataformat.csv.impl.CsvIOContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

class JacksonFileAppender implements FileAppender {

    private final CsvEncoder csvEncoder;

    JacksonFileAppender(Path filePath) {
        try {
            var writer = new FileWriter(filePath.toFile(), StandardCharsets.UTF_8);
            var ctxt = new CsvIOContext(BufferRecyclers.getBufferRecycler(), writer, false);
            int csvFeatures = CsvGenerator.Feature.collectDefaults();
            csvEncoder = new CsvEncoder(
                ctxt,
                csvFeatures,
                writer,
                CsvSchema.emptySchema()
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void append(long value) throws IOException {
        csvEncoder.write(csvEncoder.nextColumnIndex(), value);
    }

    @Override
    public void append(double value) throws IOException {
        csvEncoder.write(csvEncoder.nextColumnIndex(), value);
    }

    @Override
    public void append(String value) throws IOException {
        csvEncoder.write(csvEncoder.nextColumnIndex(), value);
    }

    @Override
    public void append(double[] value) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(value[0]);
        for (int i = 1; i < value.length; i++) {
            sb.append(";").append(value[i]);
        }
        append(sb.toString());
    }

    @Override
    public void append(long[] value) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(value[0]);
        for (int i = 1; i < value.length; i++) {
            sb.append(";").append(value[i]);
        }
        append(sb.toString());
    }

    @Override
    public void append(float[] value) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(value[0]);
        for (int i = 1; i < value.length; i++) {
            sb.append(";").append(value[i]);
        }
        append(sb.toString());
    }

    @Override
    public void endLine() throws IOException {
        csvEncoder.endRow();
    }

    @Override
    public void flush() throws IOException {
        csvEncoder.flush(true);
    }

    @Override
    public void close() throws IOException {
        csvEncoder.close(true, true);
    }
}
