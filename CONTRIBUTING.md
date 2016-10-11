# Contribution

This is a small and simple contribution guide, every contribution is welcome if it is compliant with this guide.        

## Coding style

* naming convension: see http://docs.scala-lang.org/style/naming-conventions.html
* indentation is done with 2 spaces
* comments are made moreover when the code  

## Code Review Criteria

### Positives

* Fixes the root cause of a bug in existing functionality
* Adds functionality or fixes a problem needed by a large number of users
* Simple, targeted
* Maintains or improves consistency
* Easily tested; has tests
* Reduces complexity and lines of code
* Change has already been discussed and is known to committers

### Negatives, Risks

* Band-aids a symptom of a bug only
* Introduces complex new functionality, especially an API that needs to be supported
* Adds complexity that only helps a niche use case
* Adds user-space functionality that does not need to be maintained in Spark, but could be hosted externally 
* Changes a public API or semantics (rarely allowed)
* Adds large dependencies
* Changes versions of existing dependencies
* Adds a large amount of code
* Makes lots of modifications in one "big bang" change
