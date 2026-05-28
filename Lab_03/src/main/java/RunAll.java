public class RunAll {

    public static void main(String[] args) throws Exception {
        String baseDir = args.length > 0 ? args[0] : ".";
        String inputDir = baseDir + "/input";
        String outputDir = baseDir + "/output";
        String ratings = inputDir + "/ratings_*.txt";
        String movies = inputDir + "/movies.txt";
        String users = inputDir + "/users.txt";
        String occupation = inputDir + "/occupation.txt";

        int exitCode = 0;

        System.out.println("=== Running Bai1 ===");
        exitCode |= Bai1.runJob(new String[]{ratings, outputDir + "/bai1", movies});

        System.out.println("=== Running Bai2 ===");
        exitCode |= Bai2.runJob(new String[]{ratings, outputDir + "/bai2", movies});

        System.out.println("=== Running Bai3 ===");
        exitCode |= Bai3.runJob(new String[]{ratings, outputDir + "/bai3", users, movies});

        System.out.println("=== Running Bai4 ===");
        exitCode |= Bai4.runJob(new String[]{ratings, outputDir + "/bai4", users, movies});

        System.out.println("=== Running Bai5 ===");
        exitCode |= Bai5.runJob(new String[]{ratings, outputDir + "/bai5", users, occupation});

        System.out.println("=== Running Bai6 ===");
        exitCode |= Bai6.runJob(new String[]{ratings, outputDir + "/bai6"});

        if (exitCode == 0) {
            System.out.println("All tasks completed successfully.");
        } else {
            System.err.println("One or more tasks failed.");
        }

        System.exit(exitCode);
    }
}
