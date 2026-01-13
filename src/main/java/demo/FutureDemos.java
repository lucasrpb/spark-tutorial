package demo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FutureDemos {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        CompletableFuture<String> hello = CompletableFuture.supplyAsync(() -> "Hello, ");

        CompletableFuture<String> helloWorld = hello
                .thenCompose( h -> CompletableFuture.completedFuture(String.format("%s%s", h, "World!")));

        helloWorld.thenAccept(System.out::println);

        helloWorld.join();

    }

}
