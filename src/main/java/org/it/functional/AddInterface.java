package org.it.functional;

@FunctionalInterface
public interface AddInterface<T> {
    T f(T a, T b);
}

class FunctionalInterfaceExample {

    public static void main(String[] args) {

        AddInterface<Integer> addInt = (Integer a, Integer b) -> a + b;
        AddInterface<Double> addDouble = (Double a, Double b) -> a + b;

        AddInterface<Integer> multiplyInt = (Integer a, Integer b) -> a * b;

        int intResult;
        double doubleResult;

        intResult = addInt.f(1, 2);
        doubleResult = addDouble.f(1.1d, 2.2d);
        System.out.println(multiplyInt.f(1, 2));
    }
}