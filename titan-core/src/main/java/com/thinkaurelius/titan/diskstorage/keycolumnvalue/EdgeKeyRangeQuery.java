package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Created by Great on 2017/5/15.
 */
public class EdgeKeyRangeQuery extends EdgeSliceQuery {

    private final long keyStart;
    private final long keyEnd;

    public EdgeKeyRangeQuery(long keyStart, long keyEnd, long sliceStart, long sliceEnd) {
        super(sliceStart, sliceEnd);
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
    }

    public EdgeKeyRangeQuery(long keyStart, long keyEnd, EdgeSliceQuery query) {
        super(query);
        this.keyStart = keyStart;
        this.keyEnd = keyEnd;
    }

    public long getKeyStart() {
        return keyStart;
    }

    public long getKeyEnd() {
        return keyEnd;
    }

    @Override
    public EdgeKeyRangeQuery setLimit(int limit) {
        super.setLimit(limit);
        return this;
    }

    @Override
    public EdgeKeyRangeQuery updateLimit(int newLimit) {
        return new EdgeKeyRangeQuery(keyStart,keyEnd,this).setLimit(newLimit);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(keyStart).append(keyEnd).appendSuper(super.hashCode()).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        EdgeKeyRangeQuery oth = (EdgeKeyRangeQuery)other;
        return keyStart == oth.keyStart && keyEnd == oth.keyEnd && super.equals(oth);
    }

    @Override
    public String toString() {
        return String.format("KeyRangeQuery(start: %s, end: %s, columns:[start: %s, end: %s], limit=%d)",
                keyStart,
                keyEnd,
                getSliceStart(),
                getSliceEnd(),
                getLimit());
    }
}
