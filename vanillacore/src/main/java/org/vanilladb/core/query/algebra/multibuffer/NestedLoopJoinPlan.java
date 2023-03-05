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
import org.vanilladb.core.query.planner.JoinKnob;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.sql.predicate.Predicate;
import java.util.ArrayList;
import java.util.List;
import org.vanilladb.core.sql.predicate.Term.Operator;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;

/**
 * Non-recursive implementation of the hashjoin algorithm that performs hashing
 * during the preprocessing stage and merging during the scanning stage.
 */
public class NestedLoopJoinPlan extends AbstractJoinPlan {
	private Plan lhs, rhs;
	private Schema schema;
	private Histogram hist;
	private String fldName1, fldName2;
	private List<String> joinFields;
	private boolean exchange = false;//denote the direction of theta join 
	private Operator op;

	public NestedLoopJoinPlan(Plan lhs, Plan rhs, Predicate joinPredicate) {
		this.lhs = lhs;
		this.rhs = rhs;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		joinFields = findJoinFields(joinPredicate, lhs.schema(), rhs.schema());
		fldName1 = joinFields.get(0);
		fldName2 = joinFields.get(1);

		if(lhs.recordsOutput() < rhs.recordsOutput()){
			hist = joinHistogram(rhs.histogram(), lhs.histogram(), fldName2,
				fldName1);
		}else{
			hist = joinHistogram(lhs.histogram(), rhs.histogram(), fldName1,
			fldName2);
		}
		
		op = joinPredicate.getOp();
		if(exchange){
			op = reverse(op);
		}

		
	}

	@Override
	public Scan open() {
		Scan leftScan = lhs.open();
		Scan rightScan = rhs.open();
		JoinKnob.joinNumber += 1;
		
		//ensure the right side is the smaller one 
		// if(lhs.recordsOutput() < rhs.recordsOutput()){
		// 	System.out.println("in NLJ: " + fldName2 + " " + fldName1 + " " + rhs.recordsOutput() + " " + lhs.recordsOutput());
		// 	return new NestedLoopJoinScan(rightScan, leftScan, fldName2, fldName1, reverse(op));
		// }else{
			System.out.println("in NLJ: " + fldName1 + " " + fldName2 + " " + lhs.recordsOutput() + " " + rhs.recordsOutput());
			return new NestedLoopJoinScan(leftScan, rightScan, fldName1, fldName2, op);
		//}
	}

	public Operator reverse(Operator op){
		if(op == OP_GT){//> 
			return OP_LT;
		}else if(op == OP_GTE){//>= filter: rhs <= max_v
			return OP_LTE;
		}else if(op == OP_LT){//< filter: rhs > min_v
			return OP_GT;
		}else if(op == OP_LTE){//<= filter: rhs >= min_v
			return OP_GTE;
		}
		return op;
	}

	/**
	 * Returns the number of block acceses required to nestedloop join the tables. 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return lhs.blocksAccessed() * rhs.blocksAccessed();
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
		sb.append("->NestedLoopJoinPlan (#blks=" + blocksAccessed() + ", #recs="
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
				exchange = true;
			}
		}
		return joinFields;
	}
}
