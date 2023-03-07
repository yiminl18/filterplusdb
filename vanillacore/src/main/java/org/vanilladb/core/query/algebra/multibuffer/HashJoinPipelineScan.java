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
import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import java.util.*;

public class HashJoinPipelineScan implements Scan {
	private Scan probe;
	private String hashField; //the name of field that hash table has already been built 
	private String probeField; //the name of field that probe table has 
	private Scan current = null;
	private Schema probSch; //the schema in probe side table 
	private boolean isProbeEmpty; //true is empty

	public HashJoinPipelineScan(boolean build, Scan probe, String fldname1, String fldname2, Schema sch) {
		this.probe = probe;
		this.probSch = sch;
		if(build){//left is the build table 
			this.hashField = fldname1;
			this.probeField = fldname2;
		}else{
			this.hashField = fldname2;
			this.probeField = fldname1;
		}
		//System.out.println("In hashscan: " + hashField + " " + probeField + " " + probSch.toString());
		//print debug info 
		//System.out.println("In HashJoinPipelineScan: " + build + " " + hashField + " " + HashTables.hashTables.containsKey(hashField));
	}

	@Override
	public void beforeFirst() {
		probe.beforeFirst();
		isProbeEmpty = !probe.next();
	}

	// @Override
	// public boolean next() {
	// 	if(isProbeEmpty){
	// 		return false;
	// 	}
	// 	if(current != null && current.next()){//move to the next matched tuple 
	// 		return true;
	// 	}
	// 	else if(!(isProbeEmpty = !probe.next())){//matched tuple has already been returned, but probe side is not empty
	// 		Constant value = probe.getVal(probeField);
	// 		Scan matched = HashTables.Probe(hashField, value);
	// 		if(matched == null){//there is no matched tuples for current probe record, move to next probe record 
	// 			return next();
	// 		}else{
	// 			//System.out.println("Print in Hash Join: " + value);
	// 			openscan(matched);
	// 			return current.next();
	// 		}
	// 	}else{
	// 		return false;
	// 	}
	// }

	@Override
	public boolean next() {
		if(isProbeEmpty){
			return false;
		}
		if(current != null && current.next()){//move to the next matched tuple 
			//System.out.println("In hashscan: 1");
			return true;
		}
		else if(!(isProbeEmpty = !probe.next())){//matched tuple has already been returned, but probe side is not empty
			
			// filterPlan.printFilter();
			//move to next valid probe
			while(!filterPlan.checkFilter(probe)){
				//System.out.println("in hashscan: probe failed filter check!");
				if(!probe.next()){
					isProbeEmpty = false;
					return false;
				}				
			}
			// if(!filterPlan.checkFilter(probe)){//current probe tuple is invalid, move to next valid probe record 
			// 	current = null;
			// 	return next();
			// }
			//System.out.println("In HashJoinScan: " + probSch.toString());
			Constant value = probe.getVal(probeField);
			//System.out.println("In hashscan: " + value.toString());
			Scan matched = HashTables.Probe(hashField, value);
			if(matched == null){//there is no matched tuples for current probe record, move to next probe record 
				current = null;
				return next();
			}else{
				//System.out.println("Print in Hash Join: " + value);
				openscan(matched);
				return current.next();
			}
		}else{
			return false;
		}
	}

	// @Override
	// public boolean next() {
	// 	if(isProbeEmpty){//if probe is complete 
	// 		return false;
	// 	}
	// 	if(current != null && current.next()){//move to the next matched tuple 
	// 		return true;
	// 	}
	// 	//move to next *valid* probe record 
	// 	while(true){
	// 		isProbeEmpty = !probe.next();
	// 		if(!isProbeEmpty){
	// 			if(filterPlan.checkFilter(probe)){//true means probe tuple passes test, then do join
	// 				Constant value = probe.getVal(probeField);
	// 				Scan matched = HashTables.Probe(hashField, value);
	// 				if(matched == null){//there is no matched tuples for current probe record, move to next probe record 
	// 					return next();
	// 				}else{
	// 					openscan(matched);
	// 					return current.next();
	// 				}
	// 			}else{//current probe tuple is invalid, move to next probe tuple 
	// 				continue;
	// 			}
	// 		}else{
	// 			return false;
	// 		}
	// 	}
	// }

	@Override
	public void close() {
		if(current!=null){
			current.close();
		}
		if(probe != null){
			probe.close();
		}
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
		//first copy Probe record into a new VirtualScan
		VirtualScan vs = new VirtualScan(probSch);
		vs.insert(copyRecord(probe, probSch));
		//join with matched records in the hash table 
		current = new ProductScan(vs,matched); 
		current.beforeFirst();
	}

	public VirtualRecord copyRecord(Scan src, Schema sch) {
        HashMap<String, Constant> record = new HashMap<>();
        for (String fldname : sch.fields())
            record.put(fldname, src.getVal(fldname));
        return new VirtualRecord(record);
    }
}
