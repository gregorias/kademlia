Overview
========

A customizable implementation of kademlia DHT protocol in java.

Building
========

Kademlia uses gradle with java plugin for typical java build operations and
a typical gradle workflow will build this project. Below is explanation
of some idiosyncrasies. *READ* them before trying to use this library.

dfuntesting dependency
----------------------
Temporarily kademlia directly depends on dfuntesting library for testing.
Unfortunately it's not published anywhere and currently is only a project on
Github. This *WILL* be changed in the future, but for now it is required to
include it into gradle multiproject build. Assuming kademlia repository is in
`workspace/kademlia` and it's the current directory run the following:

* `cd ../`
* clone dfuntesting from github's `gregorias/dfuntesting`
* `touch settings.gradle; echo < "include 'dfuntesting', 'kademlia'"`


copyAllDependencies task
------------------------
Run `./gradlew copyAllDependencies' to copy all kademlia dependencies into
`allLibs` directory.


Build basic kademlia application with REST interface
----------------------------------------------------
Kademlia comes with REST interface running on Grizzly HTTP server. To build
this run:

 `./gradlew jar`

`build/libs/kademlia.jar` file should be created.  This jar already comes with
manifest which expects all dependent jars to be present in `lib` directory.

To run this application use:
`java -jar kademlia.jar resources/config/kademlia.xml`
