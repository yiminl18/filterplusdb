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
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import java.util.*;

import org.vanilladb.core.storage.metadata.GlobalInfo;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.TableStatInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class corresponding to a table.
 */
public class TablePlan implements Plan {
	private Transaction tx;
	private TableInfo ti;
	private TableStatInfo si;

	/**
	 * Creates a leaf node in the query tree corresponding to the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the calling transaction
	 */
	public TablePlan(String tblName, Transaction tx) {
		this.tx = tx;
		ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		if (ti == null)
			throw new TableNotFoundException("table '" + tblName
					+ "' is not defined in catalog.");
		si = VanillaDb.statMgr().getTableStatInfo(ti, tx);
	}

	/**
	 * Creates a table scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Estimates the number of block accesses for the table, which is obtainable
	 * from the statistics manager.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return si.blocksAccessed();
	}

	/**
	 * Determines the schema of the table, which is obtainable from the catalog
	 * manager.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		Schema sch = ti.schema();
		return reduceSchema(sch, GlobalInfo.queriedAttrAll);
		//return sch;
	}

	public Schema reduceSchema(Schema sch, List<String> attrs){
        Map<String, Type> fields = sch.getSchema();
        Map<String, Type> newFields = new HashMap<>();
		for(Map.Entry<String, Type> entry : fields.entrySet()){
			if(attrs.contains(entry.getKey())){
				newFields.put(entry.getKey(), entry.getValue());
			}
		}
        SortedSet<String> myFieldSet = new TreeSet<String>(newFields.keySet());
        Schema newSch = new Schema(myFieldSet, newFields);
		return newSch;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return si.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("->TablePlan on (").append(ti.tableName())
				.append(") (#blks=");
		sb.append(blocksAccessed()).append(", #recs=").append(recordsOutput())
				.append(")\n");
		return sb.toString();
	}
}
