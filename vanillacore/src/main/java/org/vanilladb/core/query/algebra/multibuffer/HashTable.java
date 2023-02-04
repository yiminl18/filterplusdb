package org.vanilladb.core.query.algebra.multibuffer;

import java.util.*;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.query.algebra.*;
/*
 * From key value to a list of records, hashMap storesa hash table. 
 * The hashed key is under fieldName  
 */
public class HashTable {
    public String fieldName;
    public HashMap<Constant, Scan> hashMap;

    public HashTable(String fName){
        fieldName = fName;
        hashMap = new HashMap<>();
    }

    public void updateHashTable(Constant key, Scan src, Schema sch){
        if(hashMap.containsKey(key)){
            copyRecord(src, (UpdateScan) hashMap.get(key), sch);
        }
        else{
            Scan dest = null;
            copyRecord(src, (UpdateScan) dest, sch);
            hashMap.put(key,dest);
        }
    }

    public Scan Probe(Constant key){
        if(hashMap.containsKey(key)){
            return hashMap.get(key);
        }
        return null;
    }

    /*
     * Insert one record to the hashtable 
     */
    public void copyRecord(Scan src, UpdateScan dest, Schema sch) {
        dest.insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
    }

    public void close(){
        for (Map.Entry<Constant, Scan> entry : hashMap.entrySet()){
            entry.getValue().close();
        }
    }

}
