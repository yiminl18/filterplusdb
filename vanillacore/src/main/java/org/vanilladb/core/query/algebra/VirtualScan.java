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
package org.vanilladb.core.query.algebra;

import java.util.*;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;

public class VirtualScan implements Scan {
	private Schema sch;
	private List<VirtualRecord> records = new ArrayList<>();
	private int current = 0; 
	/**
	 * Creates a select scan having the specified underlying scan and predicate.
	 */
	public VirtualScan(Schema sch) {
		this.sch = sch;
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		current = -1;
	}

	/**
	 * Move to the next record satisfying the predicate. The method repeatedly
	 * calls next on the underlying scan until a suitable record is found, or
	 * the underlying scan contains no more records.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		current ++;
		if(current >= records.size()){
			return false;
		}
		return true;
	}

	@Override
	public void close() {
	}

	@Override
	public Constant getVal(String fldName) {
		return records.get(current).getVal(fldName);
	}

	@Override
	public boolean hasField(String fldName) {
		return sch.hasField(fldName);
	}

	public void insert(VirtualRecord vr){
		records.add(vr);
	}

	public int getSize(){
		return records.size();
	}
}
