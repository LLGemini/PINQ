Privacy Integrated Queries, v0.1.1

This project contains a prototype implementation of the PINQ language and execution system, as well as several example applications demonstrating interesting (and possibly non-obvious) uses of the system. The projects contained include:

  PINQ:			The core PINQ.dll, centered around the PINQueryable object.

  MachineLearning:	A collection of machine learning algorithms written in PINQ.

  SocialNetworking:	A few graph analyses common to social networking analysis.

  Visualization:	The example application from the SIGMOD paper, fully functional except for its inputs.

  TestHarness: 		A batch of ad-hoc computations run against the program's source file.

New projects need only add PINQ.dll as a resource, and a "using PINQ;" line to their source files. The first step in using PINQ is to create a new PINQueryable, wrapped around an IQueryable source. At this point, the programmer can begin to execute [a subset of] standard LINQ commands against the data source, culminating in any of the Noisy* aggregations.

While a more complete user manual is intended in the very near future, for the moment the author recommends the SIGMOD09 paper "Privacy Integrated Queries" as a programming companion, explaining  the [intended] functionality of the various transformations, aggregations, and other methods.

IMPORTANT: This is only prototype mock-up of the interface and underlying execution layer. While this prototype should protect data from "honest-but-curious" users, who are not actively attempting to subvert the implementation, it is NOT suitable for actual deployment at this point. A substantial amount of debugging, hardening, and general iteration on the system is still required. Instead, the prototype is intended as a exploratory programming tool, for interested users to explore the use of differential privacy in their area of expertise.

The project has been tested on Windows Vista, with Visual Studio 2008. The PINQ.dll also builds on the most recent release of Mono, but may require commenting out the ExponentialMechanism method (which appears to crash Mono's compiler).


Comments are certainly welcome about ease of use, functionality, correctness, and any other useful input.

Contact information:
Frank McSherry
mcsherry@microsoft.com


Revision history:

August 18, 2009: Aggregations now test to see that a supplied epsilon is non-negative. Oops.