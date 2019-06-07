package com.carrotsearch.hppc;

public final class OpenHashContainers {

    public static int emptyBufferSize() {
        return expectedBufferSize(Containers.DEFAULT_EXPECTED_ELEMENTS);
    }

    public static int expectedBufferSize(final int elements) {
        return HashContainers.minBufferSize(elements, (double) HashContainers.DEFAULT_LOAD_FACTOR) + 1;
    }

    private OpenHashContainers() {
        throw new UnsupportedOperationException("No instances");
    }
}
