package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.Mutation;
import com.thinkaurelius.titan.diskstorage.MyEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVMyEntryMutation;

import java.util.List;

/**
 * Created by Great on 2017/5/14.
 */
public class KCVEdgeMutation extends Mutation<MyEntry, MyEntry> {

    public KCVEdgeMutation(List<MyEntry> additions, List<MyEntry> deletions) {
        super(additions, deletions);
    }

    @Override
    public void consolidate() {
        super.consolidate(KCVMyEntryMutation.ENTRY2COLUMN_FCT, KCVMyEntryMutation.ENTRY2COLUMN_FCT);
    }

    @Override
    public boolean isConsolidated() {
        return false;
    }
}
