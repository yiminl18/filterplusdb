package org.vanilladb.core.query.algebra;

import java.util.HashMap;

import org.vanilladb.core.sql.Constant;
/*
 * This class creates a virtual record that is stored in memory. 
 */
public class VirtualRecord {
    private HashMap<String, Constant> record = new HashMap<>();

    public VirtualRecord(HashMap<String, Constant> record){
        this.record = record;
    }

    public void setVal(String fldName, Constant val){
        if(hasField(fldName)){
            record.put(fldName, val);
        }
    }

	public Constant getVal(String fldName) {
		if(hasField(fldName)){
            return record.get(fldName);
        }else{
            return null;
        }
	}

	public boolean hasField(String fldName) {
		return record.containsKey(fldName);
	}
}
