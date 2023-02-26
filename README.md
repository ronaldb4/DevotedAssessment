# DevotedAssessment
## Execution

The code has been compiled to an executable jar file: Assessment.jar file in the repo root. 

You can download that to your local machine and and execute "java -jar Assessment.jar". 

It does require a jvm that can run Java 11 (there are some supposed performance improvements). 

If you don't have Java 11 or greater on your machine, I am a fan of the OpenJDK releases (completely avoids the horror that is exposing yourself to Oracle licensing). 

Alternatively, you should be able to execute from within your preferred IDE - the com.ronaldbuchanan.assessment.ToyInMemDB.java file contains the code.

## Design

It's really stripped down: one class with a pretty dumb command interpreter.

It's all based around Java's HashMap, HashSet, and ArrayDeque data structures. These are accepted to have an average time complexity of O(1) for all of the operations performed in this exercise. 