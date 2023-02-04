/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.algebra.multibuffer;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

public class HashJoinPipelineScan implements Scan {
	private Scan probe;
	private String hashField; //the name of field that hash table has already been built 
	private Scan current = null;
	private Schema probSch; //the schema in probe side table 
	private boolean isProbeEmpty; //true is empty

	public HashJoinPipelineScan(boolean build, Scan probe, String fldname1, String fldname2, Schema sch, Transaction tx) {
		this.probe = probe;
		this.probSch = sch;
		if(build){
			this.hashField = fldname1;
		}else{
			this.hashField = fldname2;
		}
	}

	@Override
	public void beforeFirst() {
		probe.beforeFirst();
		isProbeEmpty = !probe.next();
	}

	@Override
	public boolean next() {
		if(isProbeEmpty){
			return false;
		}
		if(current.next()){//move to the next matched tuple 
			return true;
		}
		else if(!(isProbeEmpty = !probe.next())){//matched tuple has already been returned, but probe side is not empty
			Constant value = probe.getVal(hashField);
			Scan matched = HashTables.Probe(hashField, value);
			if(matched == null){//there is no matched tuples for current probe record, move to next probe record 
				return next();
			}else{
				openscan(matched);
				return current.next();
			}
		}else{
			return false;
		}
	}

	@Override
	public void close() {
		current.close();
		probe.close();
	}

	@Override
	public Constant getVal(String fldname) {
		return current.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return current.hasField(fldname);
	}

	/*
	 * Open a product scan for one lhs record and its matched rhs records from the hashtable 
	 */
	public void openscan(Scan matched){
		if(current != null){
			current.close();
		}
		//first copy Probe record into a new scan dest that only contains current record
		Scan dest = null;
		copyRecord(probe, (UpdateScan) dest, probSch);
		//join with matched records in the hash table 
		current = new ProductScan(dest,matched); 
	}

	public void copyRecord(Scan src, UpdateScan dest, Schema sch) {
        dest.insert();
        for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
    }
}
