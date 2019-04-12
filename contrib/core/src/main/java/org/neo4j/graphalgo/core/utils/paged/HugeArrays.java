package org.neo4j.graphalgo.core.utils.paged;

final class HugeArrays {

    private static final int SINGLE_PAGE_SHIFT = 28;
    static final int SINGLE_PAGE_SIZE = 1 << SINGLE_PAGE_SHIFT;

    static final int PAGE_SHIFT = 14;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

    static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    static int exclusiveIndexOfPage(long index) {
        return 1 + (int) ((index - 1L) & PAGE_MASK);
    }

    static int numberOfPages(long capacity) {
        final long numPages = (capacity + PAGE_MASK) >>> PAGE_SHIFT;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (PAGE_SIZE) + " is too small for capacity: " + capacity;
        return (int) numPages;
    }

    private HugeArrays() {
        throw new UnsupportedOperationException("No instances");
    }
}
