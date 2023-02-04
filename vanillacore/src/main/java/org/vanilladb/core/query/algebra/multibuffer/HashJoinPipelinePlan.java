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


import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Non-recursive implementation of the hashjoin algorithm that performs hashing
 * during the preprocessing stage and merging during the scanning stage.
 */
public class HashJoinPipelinePlan extends AbstractJoinPlan {
	private Plan lhs, rhs;
	private String fldName1, fldName2;
	private Transaction tx;
	private Schema schema;
	private Histogram hist;
	private boolean build; //build = true means the lhs is the build table, otherwise is false

	public HashJoinPipelinePlan(Plan lhs, Plan rhs, String fldName1, String fldName2,
			Transaction tx) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.fldName1 = fldName1;
		this.fldName2 = fldName2;
		this.tx = tx;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		hist = joinHistogram(lhs.histogram(), rhs.histogram(), fldName1,
				fldName2);
	}

	@Override
	public Scan open() {
		//build hash table for the smaller relation 
		if(lhs.blocksAccessed() < rhs.blocksAccessed()){
			//build hash table for lhs
			this.build = true;
			Scan lhsScan = lhs.open();
			lhsScan.beforeFirst();
			while(lhsScan.next()){
				HashTables.updateHashTable(fldName1,lhsScan.getVal(fldName1),lhsScan);
			}
			lhsScan.close();
			return new HashJoinPipelineScan(build, rhs.open(), fldName1, fldName2, tx);
		}else{
			//build hash table for rhs
			this.build = false;
			Scan rhsScan = lhs.open();
			rhsScan.beforeFirst();
			while(rhsScan.next()){
				HashTables.updateHashTable(fldName1,rhsScan.getVal(fldName1),rhsScan);
			}
			rhsScan.close();
			return new HashJoinPipelineScan(build, lhs.open(), fldName1, fldName2, tx);
		}
	}

	/**
	 * Returns the number of block acceses required to hashjoin the tables. It
	 * does <em>not</em> include the one-time cost of materializing and hashing
	 * the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return lhs.blocksAccessed() + rhs.blocksAccessed();
	}

	/**
	 * Returns the schema of the join, which is the union of the schemas of the
	 * underlying queries.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		return (long) hist.recordsOutput();
	}

	@Override
	public String toString() {
		String c2 = rhs.toString();
		String[] cs2 = c2.split("\n");
		String c1 = lhs.toString();
		String[] cs1 = c1.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->HashJoinPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		// right child
		for (String child : cs2)
			sb.append("\t").append(child).append("\n");
		;
		// left child
		for (String child : cs1)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
