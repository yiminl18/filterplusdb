package org.vanilladb.core.query.planner;

/*
 * This class implements knobs to control the join implementations
 */
public class JoinKnob {
    static public boolean productJoin = true, nestedloop = true, hashjoin = true;

    public static void disableProductJoin(){
        productJoin = false;
    }

    public static void disableNestLoopJoin(){
        nestedloop = false;
    }

    public static void disableHashJoin(){
        hashjoin = false;
    }
}
