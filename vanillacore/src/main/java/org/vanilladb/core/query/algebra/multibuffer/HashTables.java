package org.vanilladb.core.query.algebra.multibuffer;

import java.util.*;
import org.vanilladb.core.sql.Constant;

public class HashTables {
        public static HashMap<String, HashTable> hashTables = new HashMap<>();

        public void updateHashTable(String fieldName, Constant key, Record r){
            if(hashTables.containsKey(fieldName)){
                hashTables.get(fieldName).updateHashTable(key, r);
            }else{
                HashTable hashTable = new HashTable(fieldName);
                hashTable.updateHashTable(key, r);
                hashTables.put(fieldName, hashTable);
            }
        }

        public List<Record> Probe(String fieldName, Constant key){
            if(!hashTables.containsKey(fieldName)){
                return null;
            }
            return hashTables.get(fieldName).Probe(key);
        }
}
