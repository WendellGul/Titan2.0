package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.MyEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

/**
 * Created by Great on 2017/5/16.
 */
public interface MyKeyIterator extends RecordIterator<Long> {

    public RecordIterator<MyEntry> getEntries();
}
