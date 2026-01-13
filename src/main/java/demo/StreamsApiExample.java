package demo;

import java.util.List;
import java.util.stream.Stream;

public class StreamsApiExample {

    public static void main(String[] args) {
        var fruits = Stream.of("banana=1", "apple=3", "orange=4", "pear=5");

        fruits
                .flatMap(f -> {
                    var splitting = f.split("=");
                    var fruit = splitting[0];
                    var quantity = splitting[1];

                    return Stream.of(fruit, quantity);
                })
        .forEach(System.out::println);
    }

}
