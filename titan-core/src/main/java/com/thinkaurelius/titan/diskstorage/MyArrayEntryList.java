package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Great on 2017/5/15.
 */
public class MyArrayEntryList extends ArrayList<MyEntry> implements MyEntryList {

    @Override
    public Iterator<MyEntry> reuseIterator() {
        return null;
    }

    @Override
    public int getByteSize() {
        return 0;
    }

    @Override
    public List<Long> getColumns() {
        List<Long> columns = new ArrayList<>();
        forEach(entry -> columns.add(entry.getColumn()));
        return columns;
    }

    public static <E> MyEntryList of(Iterable<E> elements, MyEntry.GetEntry<E> getter) {
        Preconditions.checkArgument(elements!=null && getter!=null);
        MyArrayEntryList result = new MyArrayEntryList();
        int num = 0;
        for (E element : elements) {
            MyEntryList tmp = getter.getEntries(element);
            num += tmp.size();
            result.addAll(tmp);
        }
        if (num == 0)
            return EMPTY_LIST;
        return result;
    }

    public static <E> MyEntryList of(Iterator<E> elements, MyEntry.GetEntry<E> getter) {
        Preconditions.checkArgument(elements!=null && getter!=null);
        if(!elements.hasNext())
            return EMPTY_LIST;
        MyArrayEntryList result = new MyArrayEntryList();
        while(elements.hasNext()) {
            E element = elements.next();
            result.add(getter.getEntry(element));
        }
        return result;
    }
}
