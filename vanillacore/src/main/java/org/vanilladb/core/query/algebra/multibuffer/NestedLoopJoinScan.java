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
import java.util.*;

import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.planner.JoinKnob;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Term.Operator;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;
import static org.vanilladb.core.sql.predicate.Term.OP_EQ;
import org.vanilladb.core.sql.IntegerConstant;
/**
 * The Scan class for the muti-buffer version of the <em>product</em> operator.
 */
public class NestedLoopJoinScan implements Scan {
	private Scan lhsScan, rhsScan;
	private boolean isLhsEmpty;
	private boolean first = true; //used to denote if this is the first pass of rhs 
	private boolean isFirstItem = true; //used to denote if this is the first value from the first rhs scan  
	private HashMap<Constant, Boolean> memberships;
	private String fldName1, fldName2;
	private Operator op;
	private boolean isThetaJoin; 
	Constant max_v = null, min_v = null;

	/**
	 * Creates the scan class for the product of the LHS scan and a table.
	 * 
	 * @param lhsScan
	 *            the LHS scan
	 * @param lhsScan
	 *            the RHS scan
	 * @param tx
	 *            the current transaction
	 */
	public NestedLoopJoinScan(Scan lhsScan, Scan rhsScan, String fldName1, String fldName2, Operator op) {
		this.lhsScan = lhsScan;
		this.rhsScan = rhsScan;
		this.fldName1 = fldName1;
		this.fldName2 = fldName2;
		this.op = op;
		memberships = new HashMap<>();
		if(op == OP_EQ){
			isThetaJoin = false;
		}else{
			isThetaJoin = true;
		}
		
		
		System.out.println("in NLJ: " + fldName1 + " " + fldName2);
	}

	/**
	 * Positions the scan before the first record. That is, the LHS scan is
	 * positioned at its first record, and the RHS scan is positioned before the
	 * first record of the first chunk.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		//System.out.println("======== " + fldName1 + " " + fldName2);
		lhsScan.beforeFirst();
		rhsScan.beforeFirst();
		isLhsEmpty = !lhsScan.next();
		
	}

	public void addItem(Constant val){
		if(!memberships.containsKey(val)){
			memberships.put(val, true);
		}
	}

	public void updateThetaFilter(Constant value){
		if(value.compareTo(max_v) > 0){
			max_v = value;
		}
		if(value.compareTo(min_v) < 0){
			min_v = value;
		}
	}

	public void createMembershipFilter(){
		filterPlan.addFilter(fldName1, "membership", memberships);
		System.out.println("in NLJ: " + fldName1);
		filterPlan.addFilter(fldName2, "membership", memberships);
		//also create range filter from equal join for fldName1 only 
		filterPlan.addFilter(fldName1, "equalrange", null, null, min_v, max_v, true, true, true, true);
	}

	public void createThetaJoinFilter(){
		if(op == OP_GT){//> filter: lhs > min_v
			filterPlan.addFilter(fldName1, "range", null, null, min_v, new IntegerConstant(0), false, false, true, false);
		}else if(op == OP_GTE){//>= filter: lhs >= min_v
			filterPlan.addFilter(fldName1, "range", null, null, min_v, new IntegerConstant(0), true, false, true, false);
		}else if(op == OP_LT){//< filter: lhs < max_v
			filterPlan.addFilter(fldName1, "range", null, null, new IntegerConstant(0), max_v, false, false, false, true);
		}else if(op == OP_LTE){//<= filter: lhs <= max_v
			filterPlan.addFilter(fldName1, "range", null, null, new IntegerConstant(0), max_v, false, true, false, true);
		}
	}

	/**
	 *	For each lhs record, iterate all rhs record. When rhs is complete, move the 
	 * the next lhs record. When lhs is complete, join is done.
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)
			return false;
		if (rhsScan.next()){
			if(first){
				Constant val = rhsScan.getVal(fldName2);
				if(!isThetaJoin){
					addItem(val);
				}
				if(isFirstItem){
					max_v = val;
					min_v = val;
					isFirstItem = false;
				}else{
					updateThetaFilter(val);
				}
			}
			return true;
		}
		else if (!(isLhsEmpty = !lhsScan.next())) {//rhs is empty but but Lhs is not empty
			if(first){
				//System.out.println("in NLJ scan: right scan ends! " + fldName1 + " " + fldName2);
				if(!isThetaJoin){
					createMembershipFilter();
				}else{
					createThetaJoinFilter();
				}
				first = false;
				JoinKnob.completeScanNumber += 1;
			}
			rhsScan.beforeFirst();
			return rhsScan.next();
		} else {
			return false;
		}
	}

	/**
	 * Closes the current scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		lhsScan.close();
		rhsScan.close();
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldname) {
		//System.out.println("Print in NLJ: "+ fldname);
		if (lhsScan.hasField(fldname)){
			//System.out.println("Print in NLJ: "+ fldname);
			return lhsScan.getVal(fldname);
		}
		else if(rhsScan.hasField(fldname)){
			return rhsScan.getVal(fldname);
		}
		return null;	
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return lhsScan.hasField(fldName) || rhsScan.hasField(fldName);
	}

	
}
