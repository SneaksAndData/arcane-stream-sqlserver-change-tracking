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

> [!NOTE]  
> The action pushes the image to GitHub Container Registry, and it is not advised to push the debug image to the 
> AWS public ECR repository to avoid publishing too much debug tags to the AWS repository.

When we are done and the stream is running using the debug image, we can connect to the pod using kubectl or other
tools.

# Collecting stack traces
If the stream runner is hanging, we can collect a thread dump to see what is happening in the application.
To do that, we can use the jstack command available in the debug image.

If Arcane is running in a container with PID 1, we can use the following command to collect a thread dump:

```
/usr/lib/jvm/temurin21_jdk_arm64/bin/jstack 1 > thread_dump.txt
```

When collected, thread dump can be moved to the local machine using the `kubectl cp` command.

We can easily analyze the thread dump using
[Intellij IDEA](https://www.jetbrains.com/help/idea/analyzing-external-stacktraces.html) or other tools.

# Collecting heap dumps
If the stream runner is using too much memory, we can collect a heap dump to see what the memory is used for.

As in the example above, we use the jmap command available in the debug image and collect a heap dump for the PID 1:

```
/usr/lib/jvm/temurin21_jdk_arm64/bin/jmap -dump:format=b,file=heap.hprof 1
```

> [!WARNING]  
> Ensure that you have enough disk space in the pod to collect the heap dump.

We can analyze the heap dump using [Eclipse MAT](https://www.eclipse.org/mat/),
[Intellij IDEA](https://www.jetbrains.com/help/idea/create-a-memory-snapshot.html#-cww8bx_8) or other tools.
