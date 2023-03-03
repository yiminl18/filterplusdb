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

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.MaterializePlan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.filter.filterPlan;
import org.vanilladb.core.sql.predicate.Term.Operator;
import static org.vanilladb.core.sql.predicate.Term.OP_GT;
import static org.vanilladb.core.sql.predicate.Term.OP_GTE;
import static org.vanilladb.core.sql.predicate.Term.OP_LT;
import static org.vanilladb.core.sql.predicate.Term.OP_LTE;

/**
 * The {@link Plan} class for the muti-buffer version of the <em>product</em>
 * operator.
 */
public class MultiBufferProductPlan implements Plan {
	private Plan lhs, rhs;
	private Transaction tx;
	private Schema schema;
	private Histogram hist;
	private boolean isThetaJoin;
	private String fldName1, fldName2;
	private List<String> joinFields;
	private Operator op;
	private boolean exchange = false;//denote the direction of theta join 

	/**
	 * Creates a product plan for the specified queries.
	 * 
	 * @param lhs
	 *            the plan for the LHS query
	 * @param rhs
	 *            the plan for the RHS query
	 * @param tx
	 *            the calling transaction
	 */
	public MultiBufferProductPlan(Plan Lhs, Plan Rhs, Transaction tx, Predicate joinPredicate) {
		// System.out.println("in multibuffer: " + Lhs.blocksAccessed() + " " + Rhs.blocksAccessed() + " " + joinPredicate.toString());
		// System.out.println(Lhs.recordsOutput() +  " " + Rhs.recordsOutput());
		// System.out.println(Lhs.schema().toString() + " " + Rhs.schema().toString());
		if(Lhs.recordsOutput() < Rhs.recordsOutput()){
			//put the smaller table in the right side 
			this.rhs = Lhs;
			this.lhs = Rhs;
		}else{
			this.lhs = Lhs;
			this.rhs = Rhs;
		}
		this.tx = tx;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		hist = ProductPlan.productHistogram(lhs.histogram(), rhs.histogram());
		isThetaJoin = joinPredicate.isThetaJoin();
		joinFields = findJoinFields(joinPredicate, lhs.schema(), rhs.schema());
		fldName1 = joinFields.get(0);
		fldName2 = joinFields.get(1);
		op = joinPredicate.getOp();
		if(exchange){
			op = reverse(op);
		}
		//System.out.println("in multibuffer scan: " +  fldName1 + " " + fldName2 + " " + op);
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
	 * A scan for this query is created and returned, as follows. First, the
	 * method materializes its RHS query. It then determines the optimal chunk
	 * size, based on the size of the materialized file and the number of
	 * available buffers. It creates a chunk plan for each chunk, saving them in
	 * a list. Finally, it creates a multiscan for this list of plans, and
	 * returns that scan.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		TempTable tt = copyRecordsFrom(rhs);
		TableInfo ti = tt.getTableInfo();
		//createFilter();
		Scan leftscan = lhs.open();
		return new MultiBufferProductScan(leftscan, ti, tx);
	}

	public void createFilter(){
		//create filter in left side only for equal join 
		if(!isThetaJoin){//if this is equal join 
			Scan leftscan = lhs.open();
			leftscan.beforeFirst();
			HashMap<Constant, Boolean> membership = new HashMap<>();
			while(leftscan.next()){
				Constant value = leftscan.getVal(fldName1);
				if(!membership.containsKey(value)){
					membership.put(value,true);
				}
			}
			leftscan.close();
			filterPlan.addFilter(fldName1, "membership", membership);
			filterPlan.addFilter(fldName2, "membership", membership);
		}else{//if this is theta-join
			if(op!=null){
				boolean isleft = false;
				Constant max_v = null, min_v = null;
				boolean first = false;
				//create filters in the smaller relation to help filter away larger relation 
				if(lhs.recordsOutput() < rhs.recordsOutput()){
					//scan left relation 
					isleft = true;
					Scan leftscan = lhs.open();
					leftscan.beforeFirst();
					while(leftscan.next()){
						Constant value = leftscan.getVal(fldName1);
						if(!first){//assign initial values of max_v, min_v
							max_v = value;
							min_v = value;
							first = true;
						}
						if(value.compareTo(max_v) > 0){
							max_v = value;
						}
						if(value.compareTo(min_v) < 0){
							min_v = value;
						}
					}
					leftscan.close();
				}
				else{
					//scan right relation 
					Scan rightscan = rhs.open();
					rightscan.beforeFirst();
					
					while(rightscan.next()){
						Constant value = rightscan.getVal(fldName2);
						if(!first){//assign initial values of max_v, min_v
							max_v = value;
							min_v = value;
							first = true;
						}
						if(value.compareTo(max_v) > 0){
							max_v = value;
						}
						if(value.compareTo(min_v) < 0){
							min_v = value;
						}
					}
					rightscan.close();
				}
				
				//create filter from theta join
				if(isleft){//use fitlers created in the left side to filter right side
					if(op == OP_GT){//> filter: rhs < max_v
						filterPlan.addFilter(fldName2, "range", null, null, new IntegerConstant(0), max_v, false, false, false, true);
					}else if(op == OP_GTE){//>= filter: rhs <= max_v
						filterPlan.addFilter(fldName2, "range", null, null, new IntegerConstant(0), max_v, false, true, false, true);
					}else if(op == OP_LT){//< filter: rhs > min_v
						filterPlan.addFilter(fldName2, "range", null, null, min_v, new IntegerConstant(0), false, false, true, false);
					}else if(op == OP_LTE){//<= filter: rhs >= min_v
						filterPlan.addFilter(fldName2, "range", null, null, min_v, new IntegerConstant(0), true, false, true, false);
					}
				}else{///use fitlers created in the right side to filter left side
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
			}
		}
	}

	/**
	 * Returns an estimate of the number of block accesses required to execute
	 * the query. The formula is:
	 * 
	 * <pre>
	 * B(product(p1, p2)) = B(p2) + B(p1) * C(p2)
	 * </pre>
	 * 
	 * where C(p2) is the number of chunks of p2. The method uses the current
	 * number of available buffers to calculate C(p2), and so this value may
	 * differ when the query scan is opened.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// this guesses at the # of chunks
		int avail = tx.bufferMgr().available();
		long size = new MaterializePlan(rhs, tx).blocksAccessed();
		long numchunks = size / avail;
		return rhs.blocksAccessed() + (lhs.blocksAccessed() * numchunks);
	}

	/**
	 * Returns the schema of the product, which is the union of the schemas of
	 * the underlying queries.
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
		sb.append("->MultiBufferProductPlan (#blks=" + blocksAccessed()
				+ ", #recs=" + recordsOutput() + ")\n");
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

	private TempTable copyRecordsFrom(Plan p) {
		Scan src = p.open();
		Schema sch = p.schema();
		TempTable tt = new TempTable(sch, tx);
		UpdateScan dest = (UpdateScan) tt.open();
		src.beforeFirst();
		boolean first = false;
		Constant max_v = null, min_v = null;
		HashMap<Constant, Boolean> membership = new HashMap<>();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields()){
				dest.setVal(fldname, src.getVal(fldname));
			}
			Constant value = src.getVal(fldName2);
			//update membership filter in the right side for equal join
			if(!isThetaJoin){
				if(!membership.containsKey(value)){
					membership.put(value,true);
				}
			}//maintain info for theta filter 
			else{
				if(!first){//assign initial values of max_v, min_v
					max_v = value;
					min_v = value;
					first = true;
				}
				if(value.compareTo(max_v) > 0){
					max_v = value;
				}
				if(value.compareTo(min_v) < 0){
					min_v = value;
				}
			}
		}
		src.close();
		dest.close();
		if(!isThetaJoin){//create fitler for equal join
			filterPlan.addFilter(fldName1, "membership", membership);
			filterPlan.addFilter(fldName2, "membership", membership);
		}else{//create filter for theta join 
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
		return tt;
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
