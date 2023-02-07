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

import org.vanilladb.core.sql.Constant;
/**
 * The scan class corresponding to the <em>product</em> relational algebra
 * operator.
 */
public class ProductScan implements Scan {
	private Scan s1, s2;
	private boolean isLhsEmpty;

	/**
	 * Creates a product scan having the two underlying scans.
	 * 
	 * @param s1
	 *            the LHS scan
	 * @param s2
	 *            the RHS scan
	 */
	public ProductScan(Scan s1, Scan s2) {
		this.s1 = s1;
		this.s2 = s2;
		// System.out.println("In ProductScan: print s1 and s2");
		// s1.beforeFirst();
		// while(!s1.next()){
		// 	if(s1.hasField("cid")){
		// 		System.out.println(s1.getVal("cid"));
		// 	}
		// }
		// s1.beforeFirst();
		// s2.beforeFirst();
		// while(!s2.next()){
		// 	if(s2.hasField("cid")){
		// 		System.out.println(s2.getVal("cid"));
		// 	}
		// }
		// s2.beforeFirst();
		// System.out.println("In ProductScan: end");
	}

	/**
	 * Positions the scan before its first record. In other words, the LHS scan
	 * is positioned at its first record, and the RHS scan is positioned before
	 * its first record.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		s1.beforeFirst();
		isLhsEmpty = !s1.next();
		s2.beforeFirst();
	}

	/**
	 * Moves the scan to the next record. The method moves to the next RHS
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first RHS record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)
			return false;
		// the old method
		if (s2.next()){
			return true;
		}
		else if (!(isLhsEmpty = !s1.next())) {//rhs is empty but but Lhs is not empty
			s2.beforeFirst();
			return s2.next();
		} else {
			return false;
		}
		// the new code s
		// while(!s2.next()){
		// 	if(!filterPlan.checkFilter(s2)){//if current tuple failed filter test, check next tuple
		// 		System.out.println("s2 failed in 1!");
		// 		continue;
		// 	}
		// 	//current tuple pass filter test 
		// 	return true;
		// }
		// if(!(isLhsEmpty = !s1.next())){//Lhs is not empty
		// 	s2.beforeFirst();
		// 	while(!s2.next()){
		// 		if(!filterPlan.checkFilter(s2)){//if current tuple failed filter test, check next tuple
		// 			System.out.println("s2 failed in 2!");
		// 			continue;
		// 		}
		// 		return true;
		// 	}
		// 	return false;
		// }
		// else{
		// 	return false;
		// }
		
	}

	/**
	 * Closes both underlying scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s1.close();
		s2.close();
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (s1.hasField(fldName))
			return s1.getVal(fldName);
		else
			return s2.getVal(fldName);
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return s1.hasField(fldName) || s2.hasField(fldName);
	}
}
