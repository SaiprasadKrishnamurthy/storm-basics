## A Simple example using Apache Storm ##

# Use Case #
## Real Time Movie Trending For a World-Wide Event ##
There is a world-wide trending on Movies showing in different parts of the world at the same time.
Watchers show up to the cinemas and get their identities verified first.
Upon successful identity verification, they're allowed to proceed for the event.
Sounds simple.

We'd like to get a real-time trending upon the following:
* Count of Watchers by Gender.
* Count of Watchers by Nationality.
* Count of Watchers by Place Of Birth.

We want to apply these trend functions on the real-time data that is streaming through and display the results.
 
## Storm Topology Modelling ##

Out Topology Looks like this:

There are two Pipelines:
**Pipeline 1:**
```
WatchersSpout --> WatchersIdentityVerificationBolt --> GenderTrendingBolt --> DataStore (Database)
                                                           --> NationalityTrendingBolt --> DataStore (Database)
                                                           --> PlaceOfBirthTrendingBolt --> DataStore (Database) 
```
As you can see the three trending happens in parallel.
WatchersIdentityVerificationBolt has multiple workers (12) to improve throughput.

**Pipeline 2:**
```
EventsRealTimeTrendSpout --> LoggingBolt
```
This is the spout that reads from the database and constantly prints the results.


## To Run ##
Simply run the moviestrending/Main.java as a standalone java program. You will see the trends constantly streamed to the console.

## Another one: ##

Real time traffic monitoring report.
