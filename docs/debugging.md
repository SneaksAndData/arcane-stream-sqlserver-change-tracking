# How to debug


# Collecting stack traces
JStack

```
/usr/lib/jvm/temurin21_jdk_arm64/bin/jstack
```

# Collecting heap dumps
JMap

```
/usr/lib/jvm/temurin21_jdk_arm64/bin/jmap -dump:format=b,file=heap.hprof 1
```
