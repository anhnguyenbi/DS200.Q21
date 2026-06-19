public class RunAll {

    public static void main(String[] args) throws Exception {
        String baseDir = args.length > 0 ? args[0] : ".";
        String inputDir = baseDir + "/input";
        String outputDir = baseDir + "/output";

        int exitCode = 0;

        System.out.println("=== Running Bai1 ===");
        exitCode |= Bai1.runJob(inputDir, outputDir + "/bai1");

        System.out.println("=== Running Bai2 ===");
        exitCode |= Bai2.runJob(inputDir, outputDir + "/bai2");

        System.out.println("=== Running Bai3 ===");
        exitCode |= Bai3.runJob(inputDir, outputDir + "/bai3");

        System.out.println("=== Running Bai4 ===");
        exitCode |= Bai4.runJob(inputDir, outputDir + "/bai4");

        System.out.println("=== Running Bai5 ===");
        exitCode |= Bai5.runJob(inputDir, outputDir + "/bai5");

        System.out.println("=== Running Bai6 ===");
        exitCode |= Bai6.runJob(inputDir, outputDir + "/bai6");

        System.out.println("=== Running Bai7 ===");
        exitCode |= Bai7.runJob(inputDir, outputDir + "/bai7");

        System.out.println("=== Running Bai10 ===");
        exitCode |= Bai10.runJob(inputDir, outputDir + "/bai10");

        if (exitCode == 0) {
            System.out.println("All tasks completed successfully.");
        } else {
            System.err.println("One or more tasks failed.");
        }

        System.exit(exitCode);
    }
}
