Errors
---------

Errors provides a package for wrapping go based errors with location of return site.


## Install

```bash
go get -v github.com/gokit/errors
```

## Usage

1. Create new errors


```go
newBadErr = errors.New("failed connection: %s", "10.9.1.0")
```

2. Create new errors with stacktrace


```go
newBadErr = errors.Stacked("failed connection: %s", "10.9.1.0")
```

3. Wrap existing error


```go
newBadErr = errors.Wrap(BadErr, "something bad happened here")
```

4. Wrap existing error with stacktrace


```go
newBadErr = errors.WrapStack(BadErr, "something bad happened here")
```

5. Add Stack to package's error type without stack trace.


```go
newBadErr = errors.StackIt(BadErr)
```
