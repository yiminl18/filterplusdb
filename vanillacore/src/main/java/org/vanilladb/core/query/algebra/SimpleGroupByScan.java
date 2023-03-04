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

import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.query.algebra.materialize.GroupValue;
import org.vanilladb.core.query.planner.JoinKnob;

/**
 * The Scan class for the <em>groupby</em> operator.
 */
public class SimpleGroupByScan implements Scan {
	private Scan ss;
	private Collection<String> groupFlds;
	private Constant gval; 
	private Collection<AggregationFn> aggFns;
	private boolean moreGroups;
	private String gFld = null;
	private HashMap<Constant, List<AggregationFn>> aggFnsMap = new HashMap<>();
	private List<AggregationFn> currentAggFns; 
	private Constant currentGVal;
	private Iterator<Map.Entry<Constant, List<AggregationFn>>> iter = null;
	private boolean isProcessed = false;//to indicate if the aggregation is done
	private boolean isFirst = true;

	/**
	 * Creates a groupby scan, given a grouped table scan.
	 * 
	 * @param s
	 *            the sorted scan
	 * @param groupFlds
	 *            the fields to group by. Can be empty, which means that all
	 *            records are in a single group.
	 * @param aggFns
	 *            the aggregation functions
	 */
	public SimpleGroupByScan(Scan s, Collection<String> groupFlds,
			Collection<AggregationFn> aggFns) {
		this.ss = s;
		this.groupFlds = groupFlds;
		this.aggFns = aggFns;
		Iterator<String> iterator = groupFlds.iterator();
		if(iterator.hasNext()){
			gFld = iterator.next();
		}
	}

		/*
		 * Make a deep copy of source. 
		 */
	public List<AggregationFn> createAggFns(List<AggregationFn> source){
		List<AggregationFn> copyAggs = new ArrayList<>();
		try{
			for(AggregationFn fn: source){
				copyAggs.add((AggregationFn)fn.clone());
			}
		}catch(CloneNotSupportedException c){}  
		return copyAggs; 
	}

	/**
	 * Positions the scan before the first group. Internally, the underlying
	 * scan is always positioned at the first record of a group, which means
	 * that this method moves to the first underlying record.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		ss.beforeFirst();
		moreGroups = ss.next();
	}

	/**
	 * Moves to the next group. The key of the group is determined by the group
	 * values at the current record. The method repeatedly reads underlying
	 * records until it encounters a record having a different key. The
	 * aggregation functions are called for each record in the group. The values
	 * of the grouping fields for the group are saved.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next(){
		processAggregation();
		if(JoinKnob.fastLearning && JoinKnob.ready()){
			//in fast learning phase, stops when receiving first item
			return false;
		}
		//iterate the computed aggFnsMap -- iterate each group 
		while(this.iter.hasNext()){
			Map.Entry<Constant, List<AggregationFn>> e = this.iter.next();
			currentGVal = e.getKey();
			currentAggFns = e.getValue();
			return true;
		}
		//end of the processing
		return false;
	}

	public void processAggregation(){
		if(!isProcessed){//make sure aggregation only happen one time 
			while (moreGroups = ss.next()) {
				if(JoinKnob.fastLearning && JoinKnob.ready()){
					break;
				}
				GroupValue gv = new GroupValue(ss, groupFlds);
				gval = gv.getVal(gFld);
				if(aggFns != null){
					List<AggregationFn> CaggFns = null;
					if(aggFnsMap.containsKey(gval)){
						CaggFns = createAggFns(aggFnsMap.get(gval));
					}else{//if the group field has new value, start a new aggFn
						List<AggregationFn> tempAggFns = new ArrayList<>(this.aggFns);
						CaggFns = createAggFns(tempAggFns);
						isFirst = true;
					}
					//update CaggFns
					if(isFirst){
						for (AggregationFn fn : CaggFns){
							fn.processFirst(ss);
							//add group filters for max and min
							String agg = fn.fieldName().substring(0,3);
							if(agg.equals("max")){//filter should be attr >= fn.value()
								String attr = fn.fieldName().substring(5);
								filterPlan.addFilter(attr, "groupmax", gval, gFld, fn.value(), new DoubleConstant(0), true, false, true, false);
							}
							else if(agg.equals("min")){//filter should be attr<=fn.value()
								String attr = fn.fieldName().substring(5);
								filterPlan.addFilter(attr, "groupmin", gval, gFld, new DoubleConstant(0), fn.value(), false, true, false, true);
							}
						}
						isFirst = false;
						
					}else{
						for (AggregationFn fn : CaggFns){
							fn.processNext(ss);
							//update group filters for max and min
							String agg = fn.fieldName().substring(0,3);
							if(agg.equals("max")){//filter should be attr >= fn.value()
								String attr = fn.fieldName().substring(5);
								filterPlan.updateFilter("groupmax", attr, gval, fn.value(), new IntegerConstant(0));
							}
							else if(agg.equals("min")){//filter should be attr<=fn.value()
								String attr = fn.fieldName().substring(5);
								filterPlan.updateFilter("groupmin", attr, gval, new IntegerConstant(0), fn.value());
							}
							//System.out.println("in group by scan: " + fn.value().toString() + " " + gval);
						}
					}
					//refresh hashmap
					aggFnsMap.put(gval, CaggFns);
					
				}
			}
			isProcessed = true;
			this.iter = aggFnsMap.entrySet().iterator();
		}
	}

	/**
	 * Closes the scan by closing the underlying scan.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		ss.close();
	}

	/**
	 * Gets the Constant value of the specified field. If the field is a group
	 * field, then its value can be obtained from the saved group value.
	 * Otherwise, the value is obtained from the appropriate aggregation
	 * function.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldname) {
		if (groupFlds.contains(fldname))
			return currentGVal;
		if (aggFns != null)
			for (AggregationFn fn : currentAggFns)
				if (fn.fieldName().equals(fldname)){
					return fn.value();
				}
					
		throw new RuntimeException("field " + fldname + " not found.");
	}

	/**
	 * Returns true if the specified field is either a grouping field or created
	 * by an aggregation function.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldname) {
		if (groupFlds.contains(fldname))
			return true;
		if (aggFns != null)
			for (AggregationFn fn : aggFns)
				if (fn.fieldName().equals(fldname)){
					return true;
				}

		return false;
	}
}
