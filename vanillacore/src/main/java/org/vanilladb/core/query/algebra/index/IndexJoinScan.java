/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.core.query.algebra.index;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import org.vanilladb.core.sql.Schema;
import java.util.List;
import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term.Operator;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;
import org.vanilladb.core.sql.predicate.Term;
/**
 * The scan class corresponding to the indexjoin relational algebra operator.
 * The code is very similar to that of ProductScan, which makes sense because an
 * index join is essentially the product of each LHS record with the matching
 * RHS index records.
 */
public class IndexJoinScan implements Scan {
	private Scan s;
	private TableScan ts; // the data table
	private Index idx;
	private Map<String, String> joinFields; // <LHS field -> RHS field>
	private boolean isLhsEmpty;
	private Predicate JoinPreds;
	private boolean matched_idx = false;
	private Operator op; 
	private boolean is_create = false;
	private boolean isThetaJoin;
	private List<String> joinFieldsNames;
	private String fldName1, fldName2;
	private boolean exchange = false;//denote the direction of theta join 

	/**
	 * Creates an index join scan for the specified LHS scan and RHS index.
	 * 
	 * @param s
	 *            the LHS scan
	 * @param idx
	 *            the RHS index
	 * @param joinFields
	 *            the mapping of join fields from LHS to RHS
	 * @param ts
	 *            the table scan of data table
	 */
	public IndexJoinScan(Scan s, Index idx, Map<String, String> joinFields, Predicate JoinPreds, Schema left_schema, Schema right_schema, TableScan ts) {
		this.s = s;
		this.idx = idx;
		this.joinFields = joinFields;
		this.ts = ts;
		this.JoinPreds = JoinPreds;
		joinFieldsNames = findJoinFields(JoinPreds, left_schema, right_schema);
		fldName1 = joinFieldsNames.get(0);
		fldName2 = joinFieldsNames.get(1);
		isThetaJoin = JoinPreds.isThetaJoin();
		op = JoinPreds.getOp();
		if(exchange){
			op = reverse(op);
		}
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

	public void createFilter(Operator op, String fldName, Constant value){
		if(!isThetaJoin){
			return;
		}
		//make sure to create filter one time 
		if(is_create){
			return;
		}
		is_create = true;
		if(op == OP_GT){//> filter: lhs > value
			filterPlan.addFilter(fldName, "range", value, new IntegerConstant(0), false, false, true, false);
		}else if(op == OP_GTE){//>= filter: lhs >= value
			filterPlan.addFilter(fldName, "range", value, new IntegerConstant(0), true, false, true, false);
		}else if(op == OP_LT){//< filter: lhs < value
			filterPlan.addFilter(fldName, "range", new IntegerConstant(0), value, false, false, false, true);
		}else if(op == OP_LTE){//<= filter: lhs <= value
			filterPlan.addFilter(fldName, "range", new IntegerConstant(0), value, false, true, false, true);
		}
	}

	public void updateFilter(String attr, Constant value){
		//only create for theta join 
		if(!isThetaJoin){
			return;
		}
		//the filter is on left
		if(op == OP_GT || op == OP_GTE){//> or >=
			filterPlan.updateFilter("range", attr, value, null);
		}else if(op == OP_LT || op == OP_LTE){//< or <=
			filterPlan.updateFilter("range", attr, null, value);
		}
		
	}

	/**
	 * Positions the scan before the first record. That is, the LHS scan will be
	 * positioned at its first record, and the index will be positioned before
	 * the first record for the join value.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		s.beforeFirst();
		isLhsEmpty = !s.next();// in the case that s may be empty
		if (!isLhsEmpty)
			resetIndex();
	}

	/**
	 * Moves the scan to the next record. The method moves to the next index
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first index record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)//if lhs is complete, the algorithm close
			return false;
		if (idx.next()) {//if index finds matched tuples for current lhs record, return true 
			ts.moveToRecordId(idx.getDataRecordId());
			if(!filterPlan.checkFilter(ts)){//if current tuple failed filter check, move to next record in rhs from index 
				return next();
			}
			matched_idx = true;//idx has matched tuple for current lhs probe
			return true;
		} else if (!(isLhsEmpty = !s.next())) {//lhs is not empty, move to next lhs record 
			if(!matched_idx){//there is no matched tuple from index-look-up for current lhs probe 
				//create filter on probe side
				createFilter(op, fldName1, s.getVal(fldName1));
				//update filter 
				updateFilter(fldName1, s.getVal(fldName1));
			}
			//if current lhs record does not satisfy the filter check, move to the next valid one 
			while(!filterPlan.checkFilter(s)){
				if(!s.next()){
					isLhsEmpty = false;
					return false;
				}				
			}
			resetIndex();//reset index to search for next lhs record 
			return next();//recursive calls 
		} else//if lhs is complete, the algorithm close
			return false;
	}

	/**
	 * Closes the scan by closing its LHS scan and its RHS index.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s.close();
		idx.close();
		ts.close();
	}

	/**
	 * Returns the Constant value of the specified field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (ts.hasField(fldName))
			return ts.getVal(fldName);
		else
			return s.getVal(fldName);
	}

	/**
	 * Returns true if the field is in the schema.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return ts.hasField(fldName) || s.hasField(fldName);
	}

	/*
	 * Reset the index based on the current join attribute value 
	 */
	private void resetIndex() {
		Map<String, ConstantRange> ranges = new HashMap<String, ConstantRange>();
		
		for (Map.Entry<String, String> fieldPair : joinFields.entrySet()) {
			String lhsField = fieldPair.getKey();
			String rhsField = fieldPair.getValue();
			ConstantRange range = null;
			Constant val = s.getVal(lhsField);
			if(JoinPreds.isThetaJoin()){//for theta join
				Operator op = JoinPreds.getOp();
				if(op == OP_GT){//lhs > rhs: rhs < val
					range = ConstantRange.newInstance(null, false, val, false);
				}else if(op == OP_GTE){
					range = ConstantRange.newInstance(null, false, val, true);
				}else if(op == OP_LT){//lhs < rhs: rhs > val
					range = ConstantRange.newInstance(val, false, null, false);
				}else if(op == OP_LTE){
					range = ConstantRange.newInstance(val,true,null, false);
				}
			}else{//for equal join
				range = ConstantRange.newInstance(val);
			}
			ranges.put(rhsField, range);
		}
		
		SearchRange searchRange = new SearchRange(idx.getIndexInfo().fieldNames(),
				idx.getKeyType(), ranges);
		idx.beforeFirst(searchRange);
		matched_idx = false;
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
