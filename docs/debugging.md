# How to debug

# Connecting to the pod
We are using a [distroless image](https://github.com/GoogleContainerTools/distroless
), so the production image does not contain a shell.
To debug the application, we need to use a debug image.

To be able to do that, we need to go to the .container directory and add the debug tag to the image name in the
final stage of the build:

```Dockerfile
FROM gcr.io/distroless/java21-debian12:debug
```

WHen it's done, we can build the image and run it with the debug image.
You should use the Build Container Image workflow to build and publish the image to the GitHub container registry.
Then, you can run the image with the debug image.



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
