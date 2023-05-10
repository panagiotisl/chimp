# Chimp
Chimp: Efficient Lossless Floating Point Compression for Time Series Databases

Code and reproducible tests executing Chimp and 10 earlier approaches for three data sets.

Execute (Requires Java 9):

```
mvn test
```
or individually:


```
mvn test -Dtest=TestDoublePrecision  # for double precision tests
mvn test -Dtest=TestSinglePrecision  # for single precision tests
mvn test -Dtest=TestLossy  # for lossy tests
```

### Who do I talk to? ###

* [Panagiotis Liakos](https://cgi.di.uoa.gr/~p.liakos/)
* [Katia Papakonstantinopoulou](https://www2.aueb.gr/users/katia/)
* [Yannis Kotidis](http://pages.cs.aueb.gr/~kotidis/)
