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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.Constant;

/**
 * Non-recursive implementation of the hashjoin algorithm that performs hashing
 * during the preprocessing stage and merging during the scanning stage.
 */
public class HashJoinPipelinePlan extends AbstractJoinPlan {
	private Plan lhs, rhs;
	private String fldName1, fldName2;
	private List<String> joinFields;
	private Schema schema;
	private Histogram hist;
	private boolean build; //build = true means the lhs is the build table, otherwise is false

	public HashJoinPipelinePlan(Plan lhs, Plan rhs, Predicate joinPredicate) {
		this.lhs = lhs;
		this.rhs = rhs;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		hist = joinHistogram(lhs.histogram(), rhs.histogram(), fldName1,
				fldName2);
		joinFields = findJoinFields(joinPredicate, lhs.schema(), rhs.schema());
		fldName1 = joinFields.get(0);
		fldName2 = joinFields.get(1);
	}

	@Override
	public Scan open() {
		HashMap<Constant, Boolean> membership = new HashMap<>();
		//build hash table for the smaller relation 
		System.out.println("In Hashjoin: " + fldName1 + " " + fldName2 + " " + lhs.recordsOutput() + " " + rhs.recordsOutput());
		if(lhs.recordsOutput() < rhs.recordsOutput()){//
			//build hash table for lhs
			this.build = true;
			Scan lhsScan = lhs.open();
			lhsScan.beforeFirst();
			while(lhsScan.next()){
				Constant value = lhsScan.getVal(fldName1);
				HashTables.updateHashTable(fldName1,value,lhsScan, lhs.schema());
				if(!membership.containsKey(value)){
					membership.put(value,true);
				}
			}
			lhsScan.close();
			HashTables.close(fldName1);
			//filterPlan.addFilter(fldName1, "membership", membership);
			System.out.println("In HashJoin, build table is "  + fldName1);
			filterPlan.addFilter(fldName2, "membership", membership);
			return new HashJoinPipelineScan(build, rhs.open(), fldName1, fldName2, rhs.schema());
		}else{
			//build hash table for rhs
			this.build = false;
			Scan rhsScan = rhs.open();
			rhsScan.beforeFirst();
			while(rhsScan.next()){
				Constant value = rhsScan.getVal(fldName2);
				HashTables.updateHashTable(fldName2,value,rhsScan,rhs.schema());
				if(!membership.containsKey(value)){
					membership.put(value,true);
				}
			}
			rhsScan.close();
			HashTables.close(fldName2);
			filterPlan.addFilter(fldName1, "membership", membership);
			System.out.println("In HashJoin, build table is "  + fldName2);
			//filterPlan.addFilter(fldName2, "membership", membership);
			return new HashJoinPipelineScan(build, lhs.open(), fldName1, fldName2, lhs.schema());
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
		sb.append("->HashJoinPipelinePlan (#blks=" + blocksAccessed() + ", #recs="
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

	public List<String> findJoinFields(Predicate joinPred, Schema leftSchema, Schema rightSchema){
		List<String> joinFields = new ArrayList<>();
		Term t = joinPred.getTerms().iterator().next();
		String leftJoinField = t.getlhsField();
		String rightJoinField = t.getrhsField();
		if(!leftJoinField.equals("NULL") && !rightJoinField.equals("NULL")){
			if(leftSchema.hasField(leftJoinField) && rightSchema.hasField(rightJoinField)){
				joinFields.add(leftJoinField);
				joinFields.add(rightJoinField);
			}else if(leftSchema.hasField(rightJoinField) && rightSchema.hasField(leftJoinField)){
				joinFields.add(rightJoinField);
				joinFields.add(leftJoinField);
			}
		}
		return joinFields;
	}
}
