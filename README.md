com.conductor.stream.utils
==========================
![Latest Version](https://img.shields.io/maven-central/v/com.conductor/stream-utils.svg)
![Build Status](https://shields.us-east-1.conductor.sh/jenkins/j/http/jenkins-release.infra.us-east-1.conductor.sh/opensource/stream-utils.svg)
![Tests](https://shields.us-east-1.conductor.sh/jenkins/s/http/jenkins-release.infra.us-east-1.conductor.sh/opensource/stream-utils.svg)
![Coverage](https://shields.us-east-1.conductor.sh/jenkins/t/http/jenkins-release.infra.us-east-1.conductor.sh/opensource/stream-utils.svg)

This repo contains common utilities for dealing with streams. It is intended to be very light weight, and has no external dependencies.

# Usage
These utilities are published to maven central. To use them, simply add the following dependency:

```xml
<dependency>
    <groupId>com.conductor</groupId>
    <artifactId>stream-utils</artifactId>
    <version>${LATEST_VERSION}</version>
</dependency>

```

# Utilities
For convenience, there are static functions around all of the useful utilities in this repo. Here are the utilities currently supported:

## StreamUtils
These are a series of utilities that can be used on any stream.

### `StreamUtils.buffer`
Takes a stream, and chunks it up by a given size.

#### Sample Usage
```java
Stream<List<Integer>> chunkedStream = StreamUtils.buffer(Stream.of(1, 2, 3, 4), 2);
assertEquals(chunkedStream, Stream.of(Arrays.asList(1, 2), Arrays.asList(3, 4));
```

### `StreamUtils.noCombiner`
A convenience lambda for a Stream Collector that doesn't allow the combine method to be called.

#### Sample Usage
```java
String combined = Stream.of(1, 2, 3)
    .collect(
        StringBuilder::new,
        StringBuilder::append,
        StreamUtils.noCombiner()
    ).toString();
assertEquals("123", combined);
```

### `StreamUtils.switchIfEmpty`
Takes in a stream, and a supplier for an alternate stream to be used if the given stream is empty.

#### Sample Usage
```java
Stream<Integer> stream = StreamUtils.switchIfEmpty(Stream.empty(), () -> Stream.of(1, 2, 3));
assertEquals(stream, Stream.of(1, 2, 3));
```

## OrderedStreamUtils
These are a series of utilities that can be used on an ordered stream.

### `OrderedStreamUtils.groupBy`
Takes a stream, and chunks it up by a given key, obtained by the inputted function. Assumes the stream is ordered by that key.

#### Sample Usage
```java
Stream<List<Integer>> groupedStream = OrderedStreamUtils.groupBy(Stream.of(1, 3, 5, 2, 4), (i) -> i % 2);
assertEquals(groupedStream, Stream.of(Arrays.asList(1, 3, 5), Arrays.asList(2, 4));
```

### `OrderedStreamUtils.aggregate`
Takes a stream, and chunks it up by a given key, obtained by the inputted function. Assumes the stream is ordered by that key.
It then takes the list, and transforms it into an object, given the provided aggregation function.

#### Sample Usage
```java
Stream<String> aggregatedStream = OrderedStreamUtils.aggregate(
    Stream.of(1, 3, 5, 2, 4),
    (i) -> i % 2,
    List::toString
);
assertEquals(aggregatedStream, Stream.of(Arrays.asList(1, 3, 5).toString(), Arrays.asList(2, 4).toString());
```

### `OrderedStreamUtils.sortedMerge`
Takes a collection of streams, and merges them together, using either the provided comparator function, or the natural ordering of the items.

#### Sample Usage
```java
Stream<Integer> mergedStream = OrderedStreamUtils.sortedMerge(Arrays.asList(
    Stream.of(1, 4, 7),
    Stream.of(2, 5, 8),
    Stream.of(3, 6, 9)
));
assertEquals(mergedStream, Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
```
or
```java
Stream<Integer> mergedStream = OrderedStreamUtils.sortedMerge(Arrays.asList(
    Stream.of(1, 4, 7),
    Stream.of(2, 5, 8),
    Stream.of(3, 6, 9)
), Comparators.<Integer>naturalOrder().reversed());
assertEquals(mergedStream, Stream.of(3, 2, 1, 6, 5, 4, 9, 8, 7);
```

### `OrderedStreamUtils.join`
Takes in two sorted streams, and joins them together, using the provided keying functions and comparator to determine order, the given join function to merge the two items, and the join type desired (full, inner, and left).

#### Sample Usage
```java
Stream<Integer> joinedStream = OrderedStreamUtils.join(
        Stream.of(1, 2, 3),
        Stream.of(1, 2, 3),
        Comparator.naturalOrder(),
        Function.identity(),
        Function.identity(),
        (num1, num2) -> num1 + num2,
        JoinType.OUTER
);
assertEquals(joinedStream, Stream.of(2, 4, 6));
```
There is also a builder for convenience.
```java
Stream<Integer> joinedStream = OrderedStreamUtils.join(JoinBuilder.builder()
        .setLeftHandSide(Stream.of(1, 2, 3))
        .setRightHandSide(Stream.of(1, 2, 3))
        .setOrdering(Comparator.naturalOrder())
        .setLeftHandKeyingFunction(Function.identity())
        .setRightHandKeyingFunction(Function.identity())
        .setJoinFunction((num1, num2) -> num1 + num2)
        .setJoinType(JoinType.OUTER)
);
assertEquals(joinedStream, Stream.of(2, 4, 6));
```
