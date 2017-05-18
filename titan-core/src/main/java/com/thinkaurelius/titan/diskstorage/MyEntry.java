package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.graphdb.relations.RelationCache;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Great on 2017/5/14.
 */
public interface MyEntry extends Comparable<MyEntry>, MetaAnnotated, MetaAnnotatable {

    public long getColumn();

    public long getEdgeId();

    public boolean isEdge();

    public boolean isProperty();

    public RelationCache getCache();

    public void setCache(RelationCache cache);

    public boolean hasRemaining();

    public Map<Long, Object> getSignatures();

    public static interface GetEntry<E> {

        public MyEntryList getEntries(E element);

        public MyEntry getEntry(E element);

        public Long getColumn(E element);

        public EntryMetaData[] getMetaSchema(E element);

        public Object getMetaData(E element, EntryMetaData meta);
    }

    public static GetEntry<MyEntry> ENTRY_GETTER = new GetEntry<MyEntry>() {

        @Override
        public MyEntryList getEntries(MyEntry element) {
            MyArrayEntryList re = new MyArrayEntryList();
            re.add(element);
            return re;
        }

        @Override
        public MyEntry getEntry(MyEntry element) {
            return element;
        }

        @Override
        public Long getColumn(MyEntry element) {
            return element.getColumn();
        }

        @Override
        public EntryMetaData[] getMetaSchema(MyEntry element) {
            if(!element.hasMetaData()) return new EntryMetaData[0];
            Map<EntryMetaData,Object> metas = element.getMetaData();
            return metas.keySet().toArray(new EntryMetaData[metas.size()]);
        }

        @Override
        public Object getMetaData(MyEntry element, EntryMetaData meta) {
            return element.getMetaData().get(meta);
        }

    };

    public static <E> MyEntry of(E element, GetEntry<E> getter) {
        return getter.getEntry(element);
    }

    public int getDirectionId();

}
