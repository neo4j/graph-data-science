package org.neo4j.graphalgo.core.utils.export.file.csv;

import java.io.Flushable;
import java.io.IOException;

public interface FileAppender extends Flushable, AutoCloseable {

    void append(double value) throws IOException;
    void append(double[] value) throws IOException;

    void append(long value) throws IOException;

    void append(long[] value) throws IOException;

    void append(float[] value) throws IOException;

    void append(String value) throws IOException;

    default void startLine() throws IOException {

    }

    void endLine() throws IOException;

    @Override
    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
