# Code RUN and Explanations

In /filter:
The class structures of the learned predicates are defined. 

In /query/algebra:
The index, various implementations of operations, such as join, scan, projection and aggregations, are updated by inserting predicate learning and predicate application algorithms

In /query/planner:
Add the point and knobs to turn on/off the learned predicates

In test/.../core/FullTest.java, and FullTestSuite
The test code is added to run the system by simply specifying the Query ID