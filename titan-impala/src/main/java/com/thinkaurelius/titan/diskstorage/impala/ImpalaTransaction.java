package com.thinkaurelius.titan.diskstorage.impala;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

/**
 * Created by Great on 2017/3/31.
 */
public class ImpalaTransaction extends AbstractStoreTransaction {

    public ImpalaTransaction(BaseTransactionConfig c) {
        super(c);
    }
}
