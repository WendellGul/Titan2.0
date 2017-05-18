package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Function;
import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.MyEntry;
import javax.annotation.Nullable;

import java.util.List;

/**
 * Created by Great on 2017/5/14.
 */
public class KCVMyEntryMutation extends Mutation<MyEntry, MyEntry> {

    public KCVMyEntryMutation(List<MyEntry> additions, List<MyEntry> deletions) {
        super(additions, deletions);
    }

    public static final Function<MyEntry, MyEntry> ENTRY2COLUMN_FCT = new Function<MyEntry, MyEntry>() {
        @Nullable
        @Override
        public MyEntry apply(@Nullable MyEntry entry) {
            return entry;
        }
    };

    @Override
    public void consolidate() {
        super.consolidate(ENTRY2COLUMN_FCT, ENTRY2COLUMN_FCT);
    }

    @Override
    public boolean isConsolidated() {
        return super.isConsolidated(ENTRY2COLUMN_FCT, ENTRY2COLUMN_FCT);
    }
}
