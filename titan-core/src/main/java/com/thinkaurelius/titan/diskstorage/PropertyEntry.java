package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Great on 2017/5/14.
 */
public class PropertyEntry implements MyEntry {

    private long typeId;

    private Object value;

    private TypeDefinitionCategory typeValue;

    private volatile transient RelationCache cache;

    public PropertyEntry(long typeId, Object value, TypeDefinitionCategory typeValue) {
        this.typeId = typeId;
        this.value = value;
        this.typeValue = typeValue;
    }

    public PropertyEntry(long type) {
        this.typeId = type;
        this.value = null;
        this.typeValue = null;
    }

    private Map<EntryMetaData,Object> metadata = EntryMetaData.EMPTY_METADATA;

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getPropertyValue() {
        return value;
    }

    public TypeDefinitionCategory getPropertyTypeValue() {
        return typeValue;
    }

    @Override
    public long getColumn() {
        return typeId;
    }

    @Override
    public long getEdgeId() {
        return 0;
    }

    @Override
    public boolean isEdge() {
        return false;
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean hasMetaData() {
        return !metadata.isEmpty();
    }

    @Override
    public Map<EntryMetaData, Object> getMetaData() {
        return metadata;
    }

    @Override
    public int compareTo(MyEntry o) {
        return Long.compare(typeId, o.getColumn());
    }

    @Override
    public Object setMetaData(EntryMetaData key, Object value) {
        if (metadata==EntryMetaData.EMPTY_METADATA) metadata = new EntryMetaData.Map();
        return metadata.put(key,value);
    }

    @Override
    public RelationCache getCache() {
        return cache;
    }

    @Override
    public void setCache(RelationCache cache) {
        Preconditions.checkNotNull(cache);
        this.cache = cache;
    }

    @Override
    public boolean hasRemaining() {
        return typeValue != null;
    }

    @Override
    public Map<Long, Object> getSignatures() {
        return new HashMap<>(0);
    }

    @Override
    public int getDirectionId() {
        return 0;
    }
}
