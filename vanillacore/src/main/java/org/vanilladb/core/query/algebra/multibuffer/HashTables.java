package org.vanilladb.core.query.algebra.multibuffer;

import java.util.*;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.sql.Schema;

public class HashTables {
        public static HashMap<String, HashTable> hashTables = new HashMap<>();

        public static void updateHashTable(String fieldName, Constant key, Scan r, Schema sch){
            if(hashTables.containsKey(fieldName)){
                hashTables.get(fieldName).updateHashTable(key, r, sch);
            }else{
                HashTable hashTable = new HashTable(fieldName);
                hashTable.updateHashTable(key, r, sch);
                hashTables.put(fieldName, hashTable);
            }
        }

        public static Scan Probe(String fieldName, Constant key){
            if(!hashTables.containsKey(fieldName)){
                return null;
            }
            return hashTables.get(fieldName).Probe(key);
        }

        public static void close(String fieldName){
            if(hashTables.containsKey(fieldName)){
                hashTables.get(fieldName).close();
            }
        }
}
