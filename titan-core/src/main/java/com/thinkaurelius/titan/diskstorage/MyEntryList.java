package com.thinkaurelius.titan.diskstorage;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Great on 2017/5/15.
 */
public interface MyEntryList extends List<MyEntry> {

    /**
     * Returns the same iterator as {@link #iterator()} with the only difference
     * that it reuses {@link MyEntry} objects when calling {@link java.util.Iterator#next()}.
     * Hence, this method should only be used if references to {@link Entry} objects are only
     * kept and accesed until the next {@link java.util.Iterator#next()} call.
     *
     * @return
     */
    public Iterator<MyEntry> reuseIterator();

    /**
     * Returns the total amount of bytes this entry consumes on the heap - including all object headers.
     *
     * @return
     */
    public int getByteSize();

    public List<Long> getColumns();

    public static final MyEntryList.EmptyList EMPTY_LIST = new MyEntryList.EmptyList();

    static class EmptyList extends AbstractList<MyEntry> implements MyEntryList {

        @Override
        public MyEntry get(int index) {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterator<MyEntry> reuseIterator() {
            return iterator();
        }

        @Override
        public int getByteSize() {
            return 0;
        }

        @Override
        public List<Long> getColumns() {
            return null;
        }
    }
}
