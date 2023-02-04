package org.vanilladb.core.query.algebra.multibuffer;

import java.util.*;
import org.vanilladb.core.sql.Constant;

/*
 * From key value to a list of records, hashMap storesa hash table. 
 * The hashed key is under fieldName  
 */
public class HashTable {
    public String fieldName;
    public HashMap<Constant, List<Record>> hashMap;

    public HashTable(String fName){
        fieldName = fName;
        hashMap = new HashMap<>();
    }

    public void updateHashTable(Constant key, Record r){
        if(hashMap.containsKey(key)){
            hashMap.get(key).add(r);
        }
        else{
            List<Record> records = new ArrayList<>();
            records.add(r);
            hashMap.put(key,records);
        }
    }

    public List<Record> Probe(Constant key){
        if(hashMap.containsKey(key)){
            return hashMap.get(key);
        }
        return null;
    }

}
