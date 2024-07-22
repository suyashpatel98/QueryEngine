package arrow.experimentation;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new WithArrow());
        Thread t2 = new Thread(new WithoutArrow());
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}

class WithArrow implements Runnable {
    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        String csvFile = "/Users/suyash/Desktop/work/reference/Java/query_engine/QueryEngine/src/main/java/arrow/experimentation/data.arrow"; // Assuming the Arrow file format

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             FileChannel fileChannel = FileChannel.open(Paths.get(csvFile), StandardOpenOption.READ);
             SeekableReadChannel seekableReadChannel = new SeekableReadChannel(fileChannel);
             ArrowFileReader reader = new ArrowFileReader(seekableReadChannel, allocator)) {

            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Float8Vector scoreVector = (Float8Vector) root.getVector("score");

            for (int j = 0; j < Constants.ROUNDS; j++) {
                double totalScore = 0;
                int count = 0;

                for (int i = 0; i < scoreVector.getValueCount(); i++) {
                    if (!scoreVector.isNull(i)) {
                        totalScore += scoreVector.get(i);
                        count++;
                    }
                }
                double averageScore = totalScore / count;
            }



            long endTime = System.currentTimeMillis();
            System.out.println("Total time with arrow (ms): " + (endTime - startTime));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class WithoutArrow implements Runnable {

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        String csvFile = "/Users/suyash/Desktop/work/reference/Java/query_engine/QueryEngine/src/main/java/arrow/experimentation/data.csv";
        List<Double> scores = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            // Skip the header row
            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                double score = Double.parseDouble(values[3].trim());
                scores.add(score);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < Constants.ROUNDS; i++) {
            int totalScore = 0;
            int count = 0;

            for (double score : scores) {
                totalScore += score;
                count++;
            }

            double averageScore = (double) totalScore / count;
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Total time WITHOUT arrow (ms): " + (endTime - startTime));
    }
}

