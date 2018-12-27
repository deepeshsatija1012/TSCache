package test;

import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

public class TestConcurrentSkipListSet {
	private static int getRandomNumberInRange(int min, int max) {
		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}
		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
	public static void main(String[] args) {
		ConcurrentSkipListSet<Integer> list = new ConcurrentSkipListSet<>();
		for(int i=0;i<100;i++) {
			list.add(i);
		}
		
		System.out.println(list);
		
		for(int i=0;i<10;i++) {
			int n = getRandomNumberInRange(0, 100);
			list.remove(n); list.add(n);
			System.out.println("Number : " + n);
			System.out.println("List : " + list);
		}
		
	}

}
