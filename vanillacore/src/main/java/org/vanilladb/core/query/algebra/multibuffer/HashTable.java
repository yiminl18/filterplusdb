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
    public HashMap<Constant, VirtualScan> hashMap;

    public HashTable(String fName){
        fieldName = fName;
        hashMap = new HashMap<>();
    }

    public void updateHashTable(Constant key, Scan src, Schema sch){
        if(hashMap.containsKey(key)){
            hashMap.get(key).insert(copyRecord(src, sch));
        }
        else{
            VirtualScan s = new VirtualScan(sch);
            s.insert(copyRecord(src, sch));
		    hashMap.put(key, s);
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
    public VirtualRecord copyRecord(Scan src, Schema sch) {
        HashMap<String, Constant> record = new HashMap<>();
        for (String fldname : sch.fields())
            record.put(fldname, src.getVal(fldname));
        return new VirtualRecord(record);
    }

    public void close(){
        for (Map.Entry<Constant, VirtualScan> entry : hashMap.entrySet()){
            entry.getValue().close();
        }
    }

}
