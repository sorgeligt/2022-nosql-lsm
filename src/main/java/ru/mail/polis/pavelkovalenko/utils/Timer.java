package ru.mail.polis.pavelkovalenko.utils;

public final class Timer {

    private static final String TIME_PATTERN = "?s:?ms:?mcs:?ns";
    private long startTime;

    public String elapse() {
        double elapse = System.nanoTime() - startTime;
        double times;
        String result = TIME_PATTERN;

        for (Time time : Time.times) {
            if (elapse * time.getFactor() > 0) {
                times = Math.floor(elapse * time.getFactor());
                elapse -= times * time.getFactor();
                result = result.replaceFirst("/?", String.valueOf(times));
            }
        }

        return result;
    }

    public void set() {
        startTime = System.nanoTime();
    }

    // Relatively to nanoseconds
    private enum Time {
        SECONDS(-9),
        MILLISECONDS(-6),
        MICROSECONDS(-3),
        NANOSECONDS(0);

        public static final Time[] times = values();
        private final double factor;

        Time(int degree) {
            this.factor = Math.pow(10, degree);
        }

        public double getFactor() {
            return factor;
        }
    }

}
