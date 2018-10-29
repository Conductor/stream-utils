# `stream-utils` Releases

## v1.3.0 - October 29, 2018 [maven](http://repo2.maven.org/maven2/com/conductor/stream-utils/1.3.0/) 

### Fix some issues in the code
This release fixes some issues with stream utils.

### Pull Requests Included
- [Pull 14](https://github.com/Conductor/stream-utils/pull/14) - Make sure exceptions are thrown at the appropriate call. Closes [issue #10](https://github.com/Conductor/stream-utils/issues/10)
- [Pull 15](https://github.com/Conductor/stream-utils/pull/15) - Make sure `switchIfEmpty` works when the alternate stream has a length of 1. Closes [issue #9](https://github.com/Conductor/stream-utils/issues/9)
- [Pull 16](https://github.com/Conductor/stream-utils/pull/16) - Make sure all our methods that return different streams close the base stream appropriately. Closes [issue #8](https://github.com/Conductor/stream-utils/issues/8)

### Compatibility
This change is fully backwards compatible (unless you were relying on any of these bugs).


## v1.2.1 - May 31, 2018 [maven](http://repo2.maven.org/maven2/com/conductor/stream-utils/1.2.1/) 

### Update documentation
This release updates the README file to reflect the new maven central publishing, and adds a changelog. 

### Pull Requests Included
- [Pull 13](https://github.com/Conductor/stream-utils/pull/13) - Update Readme and add Changelog

### Compatibility
This change is fully backwards compatible.  


## v1.2.0 - May 31, 2018 [maven](http://repo2.maven.org/maven2/com/conductor/stream-utils/1.2.0/) 

### Publish to Maven Central
This release add publishing to maven central. 

### Pull Requests Included
- [Pull 11](https://github.com/Conductor/stream-utils/pull/11) - getting ready for Nexus
- [Pull 12](https://github.com/Conductor/stream-utils/pull/12) - fix scm block

### Compatibility
This change is fully backwards compatible.  


## v1.1.0 - August 31, 2017 [jar](https://github.com/Conductor/stream-utils/releases/download/v1.1.0/stream-utils-1.1.0.jar) 

### Fix a possible NullPointerException in the join
This release fixes the NPE that can arise from passing in an empty stream on one side of the join, if your comparator doesn't handle nulls gracefully. This is convenient because it allows you to use `Ordering.natural()`. 

### Pull Requests Included
- [Pull 5](https://github.com/Conductor/stream-utils/pull/5) - Handle empty streams properly

### Compatibility
This change is fully backwards compatible.    


## v1.1.0 - August 15, 2017 [jar](https://github.com/Conductor/stream-utils/releases/download/v1.1.0/stream-utils-1.1.0.jar) 

### Add a builder for ease of constructing joins
 
This release adds a builder for join construction. 

### Pull Requests Included
- [Pull 3](https://github.com/Conductor/stream-utils/pull/3) - Add builder to the Join function 

### Compatibility
This change is fully backwards compatible.  


## v1.0.0 - August 2, 2017 [jar](https://github.com/Conductor/stream-utils/releases/download/v1.0.0/stream-utils-1.0.0.jar) 

### First release of the StreamUtils library
 
This release contains the first public version of the StreamUtils library.

Check out the README of the repo for the list of functionality contained within. 

TODO: publish this properly in Maven Central (currently in progress).
