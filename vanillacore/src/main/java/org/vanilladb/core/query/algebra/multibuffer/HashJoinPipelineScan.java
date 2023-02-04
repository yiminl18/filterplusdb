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

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class HashJoinPipelineScan implements Scan {
	private Transaction tx;
	private boolean build; //build = true means the lhs is the build table, otherwise is false
	private Scan probe;
	private String hashField; //the name of field that hash table has already been built 
	private Scan current;

	public HashJoinPipelineScan(boolean build, Scan probe, String fldname1, String fldname2, Transaction tx) {
		this.build = build;
		this.tx = tx;
		this.probe = probe;
		if(build){
			this.hashField = fldname1;
		}else{
			this.hashField = fldname2;
		}
	}

	@Override
	public void beforeFirst() {
		probe.beforeFirst();
	}

	@Override
	public boolean next() {
		
	}

	@Override
	public void close() {
		
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
	public void openscan(){

	}
}
