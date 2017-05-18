package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.MyArrayEntryList;
import com.thinkaurelius.titan.diskstorage.MyEntry;
import com.thinkaurelius.titan.diskstorage.MyEntryList;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Great on 2017/5/15.
 */
public class EdgeSliceQuery extends BaseQuery implements BackendQuery<EdgeSliceQuery> {

    private final Slice sliceStart;
    private final Slice sliceEnd;

    public EdgeSliceQuery(final long sliceStart, final long sliceEnd) {
        this.sliceStart = new Slice(sliceStart);
        this.sliceEnd = new Slice(sliceEnd);
    }

    public EdgeSliceQuery(final int dirStart, final int dirEnd) {
        this.sliceStart = new Slice(dirStart);
        this.sliceEnd = new Slice(dirEnd);
    }

    public EdgeSliceQuery(final Slice sliceStart, final Slice sliceEnd) {
        this.sliceStart = sliceStart;
        this.sliceEnd = sliceEnd;
    }

    public EdgeSliceQuery(final EdgeSliceQuery query) {
        this(query.getSliceStart(), query.getSliceEnd());
        setLimit(query.getLimit());
    }

    public Slice getSliceStart() {
        return sliceStart;
    }

    public Slice getSliceEnd() {
        return sliceEnd;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(sliceStart).append(sliceEnd).append(getLimit()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null && !getClass().isInstance(other))
            return false;

        EdgeSliceQuery oth = (EdgeSliceQuery) other;
        return sliceStart == oth.sliceStart
                && sliceEnd == oth.sliceEnd
                && getLimit() == oth.getLimit();
    }

    @Override
    public EdgeSliceQuery updateLimit(int newLimit) {
        return new EdgeSliceQuery(sliceStart, sliceEnd).setLimit(newLimit);
    }

    @Override
    public EdgeSliceQuery setLimit(int limit) {
        Preconditions.checkArgument(!hasLimit());
        super.setLimit(limit);
        return this;
    }

    public boolean contains(long l) {
        return sliceStart.getId() <= l && l < sliceEnd.getId();
    }

    public boolean subsumes(EdgeSliceQuery oth) {
        Preconditions.checkNotNull(oth);
        if (this == oth) return true;
        if (oth.getLimit() > getLimit()) return false;
        else if (!hasLimit()) //the interval must be subsumed
            return sliceStart.compareTo(oth.sliceStart) <= 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
        else //this the result might be cutoff due to limit, the start must be the same
            return sliceStart.compareTo(oth.sliceStart) == 0 && sliceEnd.compareTo(oth.sliceEnd) >= 0;
    }

    public MyEntryList getSubset(final EdgeSliceQuery otherQuery, final MyEntryList otherResult) {
        assert otherQuery.subsumes(this);
        int pos = Collections.binarySearch(otherResult.getColumns(), sliceStart.getId());
        if (pos < 0) pos = -pos - 1;

        List<MyEntry> result = new MyArrayEntryList();
        for (; pos < otherResult.size() && result.size() < getLimit(); pos++) {
            MyEntry e = otherResult.get(pos);
            if (e.getColumn() < sliceEnd.getId()
                    || (sliceEnd.getId() == -1 && e.getDirectionId() < sliceEnd.getDir()))
                result.add(e);
            else break;
        }
        return (MyEntryList) result;
    }

    public static Slice nextSlice(Slice slice) {
        if(slice.id == -1)
            return new Slice(slice.id, slice.dir + 1);
        else if(slice.dir == -1)
            return new Slice(slice.id + 1, slice.dir);
        else
            return new Slice(slice.id + 1, slice.dir + 1);
    }

    public static class Slice implements Comparable<Slice> {

        private final long id;
        private int dir;

        public Slice(long id, int dir) {
            this.id = id;
            this.dir = dir;
        }

        public Slice(long id) {
            this.id = id;
            this.dir = -1;
        }

        public Slice(int dir) {
            this.id = -1;
            this.dir = dir;
        }

        @Override
        public int compareTo(EdgeSliceQuery.Slice o) {
            if(dir == -1 || o.dir == -1)
                return Long.compare(id, o.id);
            if(id == -1 || o.id == -1) {
                if(id == -1 && o.id != -1) return -1;
                else if(id != -1) return 1;
                else return -1;
            }
            if(id != o.id)
                return Long.compare(id, o.id);
            else
                return Integer.compare(dir, o.dir);
        }

        public Long getId() {
            return id;
        }

        public int getDir() {
            return dir;
        }
    }
}
