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

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;


/**
 * The Scan class for the muti-buffer version of the <em>product</em> operator.
 */
public class NestedLoopJoinScan implements Scan {
	private Scan lhsScan, rhsScan;
	private boolean isLhsEmpty;

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
	public NestedLoopJoinScan(Scan lhsScan, Scan rhsScan) {
		this.lhsScan = lhsScan;
		this.rhsScan = rhsScan;
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
		lhsScan.beforeFirst();
		rhsScan.beforeFirst();
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
		// the old method
		if (rhsScan.next()){
			return true;
		}
		else if (!(isLhsEmpty = !lhsScan.next())) {//rhs is empty but but Lhs is not empty
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
