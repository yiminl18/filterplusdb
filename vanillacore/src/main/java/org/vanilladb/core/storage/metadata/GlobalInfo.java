// package org.vanilladb.core.storage.metadata;

// import java.util.*;
// import org.vanilladb.core.query.parse.QueryData;
// import org.vanilladb.core.sql.aggfn.AggregationFn;
// import org.vanilladb.core.sql.predicate.Term;
// import org.vanilladb.core.sql.Schema;
// /*
//  * This class is used to store the global information shared via multi-class. 
//  */
// public class GlobalInfo {
//     public static List<String> queriedAttr; //stores projected attrs and attrs in query

//     public static void getqueriedAttr(QueryData data){
//         queriedAttr = new ArrayList<>();
//         //add projected attrs
//         for (String fldName : data.projectFields()) {
//             queriedAttr.add(fldName);
//         }
//         //add aggregate attrs
//         if (data.aggregationFn() != null)
// 			for (AggregationFn aggFn : data.aggregationFn()) {
// 				String aggFld = aggFn.argumentFieldName();
//                 if(!queriedAttr.contains(aggFld)){
//                     queriedAttr.add(aggFld);
//                 }
// 			}
//         //add group by attrs
//         if (data.groupFields() != null)
//         for (String groupByFld : data.groupFields()) {
//             if(!queriedAttr.contains(groupByFld)){
//                 queriedAttr.add(groupByFld);
//             }
//         }
//         //add predicate attrs
//         for (Term term : data.pred().getTerms()){
//             String attr = term.getLhsField();
//             if(!attr.equals("null") && !queriedAttr.contains(attr)){
//                 queriedAttr.add(attr);
//             }
//             attr = term.getRhsField();
//             if(!attr.equals("null") && !queriedAttr.contains(attr)){
//                 queriedAttr.add(attr);
//             }
//         }
//     }

//     public Schema reduceSchema(Schema sch){
//         Schema newSch = new Schema();
// 		for(Map.Entry<String, Type> entry : sch.getSchema().entrySet()){
// 			if(GlobalInfo.queriedAttr.contains(entry.getKey())){
// 				//newFields.put(entry.getKey(), entry.getValue());
// 			}
// 		}
// 		return newSch;
// 	}
    
// }
