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
package org.vanilladb.core.query.planner.opt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.index.IndexJoinPlan;
import org.vanilladb.core.query.algebra.multibuffer.HashJoinPipelinePlan;
import org.vanilladb.core.query.algebra.multibuffer.MultiBufferProductPlan;
import org.vanilladb.core.query.algebra.multibuffer.NestedLoopJoinPlan;
import org.vanilladb.core.query.planner.JoinKnob;
import org.vanilladb.core.query.planner.index.IndexSelector;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * This class contains methods for planning a single table.
 */
class TablePlanner {
	private String tblName;
	private TablePlan tp;
	private Predicate pred;
	private Schema sch;
	private Transaction tx;
	private int id;
	private int hashCode;

	/**
	 * Creates a new table planner. The specified predicate applies to the
	 * entire query. The table planner is responsible for determining which
	 * portion of the predicate is useful to the table, and when indexes are
	 * useful.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param pred
	 *            the query predicate
	 * @param tx
	 *            the calling transaction
	 */
	public TablePlanner(String tblName, Predicate pred, Transaction tx, int id) {
		this.tblName = tblName;
		this.pred = pred;
		this.tx = tx;
		this.id = id;
		this.hashCode = (int) Math.pow(2, id);
		tp = new TablePlan(tblName, tx);
		sch = tp.schema();
	}
	
	/**
	 * An unique number to this planner.
	 * 
	 * @return
	 */
	public int getId() {
		return id;
	}
	
	/**
	 * Use binary to represent the combination
	 */
	@ Override
	public int hashCode() {
		return hashCode;
	}

	/**
	 * Constructs a select plan for the table. The plan will use an indexselect,
	 * if possible.
	 * 
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelectPlan();
		if (p == null)
			p = tp;
		return addSelectPredicate(p);
	}

	/**
	 * Constructs a join plan of the specified trunk and this table. The plan
	 * will use an indexjoin, if possible; otherwise a multi-buffer product
	 * join. The method returns null if no join is possible.
	 * 
	 * <p>
	 * The select predicate applicable to this table is pushed down below the
	 * join.
	 * </p>
	 * 
	 * @param trunk
	 *            the specified trunk of join
	 * @return a join plan of the trunk and this table
	 */
	public Plan makeJoinPlan(Plan trunk) {
		if(JoinKnob.fastLearning){
			//fast learning only explores basic nested loop join
			JoinKnob.disableHashJoin();
			JoinKnob.disableIndexJoin();
			JoinKnob.disableProductJoin();
		}
		Schema trunkSch = trunk.schema();
		Predicate joinPred = pred.joinPredicate(sch, trunkSch);
		if (joinPred == null)
			return null;
		if(joinPred.length() > 1){
			System.out.println("Printing in TablePlanner: There exist more than one join connecting a pair of tables");
		}
		//in the order of index plan, hash plan and product plan 
		Plan p = null;
		if(!joinPred.isThetaJoin()){//make plan for equal join
			if(JoinKnob.indexjoin){
				p = makeIndexJoinPlan(trunk, trunkSch, joinPred);
			}
			if(p != null){
				return p;
			}
			if(JoinKnob.hashjoin){
				p = makeHashJoinPlan(trunk, joinPred);
			}
			if(p != null){
				return p;
			}
			if(JoinKnob.productJoin){
				p = makeProductJoinPlan(trunk, trunkSch);//ihe: do not use product join for now 
			}
			if(p != null){
				return p;
			}
			if(JoinKnob.nestedloop){
				p = makeNestedLoopJoinPlan(trunk, joinPred);
			}
		}
		else{//make plan for theta join: prefer index join, otherwise block based NL join
			if(JoinKnob.indexjoin){
				p = makeIndexJoinPlan(trunk, trunkSch, joinPred);
			}
			if(p != null){
				System.out.println("in table planner: index scan is used");
				return p;
			}
			if(JoinKnob.productJoin){
				p = makeProductJoinPlan(trunk, trunkSch);//ihe: do not use product join for now 
			}
		}
			
		return p;
	}

	

	/**
	 * Constructs a product plan of the specified trunk and this table.
	 * 
	 * <p>
	 * The select predicate applicable to this table is pushed down below the
	 * product.
	 * </p>
	 * 
	 * @param trunk
	 *            the specified trunk of join
	 * @return a product plan of the trunk and this table
	 */
	public Plan makeProductPlan(Plan trunk) {
		Plan p = makeSelectPlan();
		Schema trunkSch = trunk.schema();
		Predicate joinPred = pred.joinPredicate(sch, trunkSch);
		System.out.print("in TablePlanner: " + joinPred.toString());
		return new MultiBufferProductPlan(trunk, p, tx, joinPred);
	}

	public Plan makeNestedLoopJoinPlan(Plan trunk, Predicate joinPredicate){
		Plan p = makeSelectPlan();
		return new NestedLoopJoinPlan(trunk, p, joinPredicate);
	}

	public Plan makeHashJoinPlan(Plan lhs, Predicate joinPredicate){
		Plan p = makeSelectPlan();
		return new HashJoinPipelinePlan(lhs, p, joinPredicate);
	}

	// public Plan makeHashPlan(Predicate joinPred, Plan trunk){
	// 	String leftJoinName = joinPred.
	// }

	/**
	 * Identify and construct an index select plan based on the predicate and
	 * indexes by examining the constants opposite to an indexed field in all
	 * terms of a predicate. Currently, fields and constants in a term cannot be
	 * moved across the term operator. Therefore this method may not identify
	 * all possible index selects. It is users' responsibility to issue queries
	 * that help the identification: e.g., "F < C", not "F - C < 0".
	 */
	private Plan makeIndexSelectPlan() {
		return IndexSelector.selectByBestMatchedIndex(tblName, tp, pred, tx);
	}

	/**
	 * Identify and construct an index join plan based on the predicate and
	 * indexes by examining the fields opposite to an indexed field in all terms
	 * of a predicate. Currently, fields and constants in a term cannot be moved
	 * across the term operator. Therefore this method may not identify all
	 * possible index joins. It is users' responsibility to issue queries that
	 * help the identification: e.g., "F1 = F2", not "F1 - F2 = 0".
	 */
	private Plan makeIndexJoinPlan(Plan trunk, Schema trunkSch, Predicate joinPred) {
		int matchedCount = 0;
		IndexInfo bestIndex = null;
		Map<String, String> bestJoinPairs = null; // <Outer Field -> Self Field>
		
		// Find the indexes that have fields joined with the target table
		Set<IndexInfo> candidates = new HashSet<IndexInfo>();
		for (String fieldName : sch.fields()) {
			Set<String> outerFlds = pred.joinFields(fieldName);
			if (outerFlds == null)
				continue;
			
			for (String outerFld : outerFlds)
				if (trunkSch.hasField(outerFld)) {
					List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblName, fieldName, tx);
					candidates.addAll(iis);
					break;
				}
		}
		
		// Find the indexes with the most joined fields
		for (IndexInfo ii : candidates) {
			if (ii.fieldNames().size() < matchedCount)
				continue;
			
			Map<String, String> joinPairs = new HashMap<String, String>();
			for (String fieldName : ii.fieldNames()) {
				Set<String> outerFlds = pred.joinFields(fieldName);
				for (String outerFld : outerFlds)
					if (trunkSch.hasField(outerFld)) {
						joinPairs.put(outerFld, fieldName);
						break;
					}
			}
			
			if (joinPairs.size() > matchedCount) {
				matchedCount = joinPairs.size();
				bestIndex = ii;
				bestJoinPairs = joinPairs;
			}
		}
		
		if (bestIndex != null) {
			Plan p = new IndexJoinPlan(trunk, tp, bestIndex, bestJoinPairs, joinPred, tx);
			/*
			 * Ideally, a select plan for this table should be
			 * created before applying the join. However, since
			 * indexjoin require the rhs plan to be a TablePlan,
			 * select predicate can only be added after the join, .
			 * which makes indexselect for this table infeasible.
			 */
			p = addSelectPredicate(p);
			return addJoinPredicate(p, trunkSch);
		}
		
		return null;
	}

	private Plan makeProductJoinPlan(Plan current, Schema currSch) {
		Plan p = makeProductPlan(current);
		return addJoinPredicate(p, currSch);
	}

	private Plan addSelectPredicate(Plan p) {//push down select predicate under the join 
		Predicate selectPred = pred.selectPredicate(sch);
		if (selectPred != null)
			return new SelectPlan(p, selectPred);
		else
			return p;
	}

	private Plan addJoinPredicate(Plan p, Schema currSch) {//add join condition on top of product plan for evaluation 
		Predicate joinPred = pred.joinPredicate(currSch, sch);
		if (joinPred != null)
			return new SelectPlan(p, joinPred);
		else
			return p;
	}
}
