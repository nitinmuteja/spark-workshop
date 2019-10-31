# _Spark-Gradle-Template_
A barebones project with scala, apache spark built using gradle. Spark-shell provides `spark` and `sc` variables pre-initialised, here I did the same using a *scala trait* that you can extend.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Gradle](https://gradle.org/)


## Build and Demo process

### Clone the Repo
`git clone git@github.com:sumedhav/spark-gradle-template.git`

### Build
`./gradlew clean build`
### Run
`./gradlew run`
### All Together
`./gradlew clean run`


## What the demo does?
Take a look at *src->main->scala->template->spark* directory

We have two Items here. 

The trait `InitSpark` which is extended by any class that wants to run spark code. This trait has all the code for initialization. I have also supressed the logging to only error levels for less noise.

The file `Main.scala` has the executable class `Main`. 
In this class, I do 4 things

- Print spark version.
- Find sum from 1 to 100 (inclusive).
- Read a csv file into a structured `DataSet`. 
- Find average age of persons from the csv.

## Using this Repo
Just import it into your favorite IDE as a gradle project. Tested with IntelliJ to work. Or use your favorite editor and build from command line with gradle.

## Libraries Included
- Spark - 2.2.4
- Scala - 2.11.12

## Useful Links
- [Spark Docs - Root Page](http://spark.apache.org/docs/latest/)
- [Spark Programming Guide](http://spark.apache.org/docs/latest/programming-guide.html)
- [Spark Latest API docs](http://spark.apache.org/docs/latest/api/)
- [Scala API Docs](http://www.scala-lang.org/api/2.12.1/scala/)
