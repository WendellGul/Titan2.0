package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Created by Great on 2017/5/15.
 */
public class EdgeKeySliceQuery extends EdgeSliceQuery {

    private final long key;

    public EdgeKeySliceQuery(long key, long sliceStart, long sliceEnd) {
        super(sliceStart, sliceEnd);
        this.key = key;
    }

    public EdgeKeySliceQuery(long key, EdgeSliceQuery query) {
        super(query);
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    @Override
    public EdgeKeySliceQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public EdgeKeySliceQuery updateLimit(int newLimit) {
        return new EdgeKeySliceQuery(key,this).setLimit(newLimit);
    }


    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).appendSuper(super.hashCode()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        EdgeKeySliceQuery oth = (EdgeKeySliceQuery)other;
        return key == oth.key && super.equals(oth);
    }

    @Override
    public String toString() {
        return String.format("KeySliceQuery(key: %s, start: %s, end: %s, limit:%d)", key, getSliceStart(), getSliceEnd(), getLimit());
    }
}
