import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gpfvic.mahout.cf.taste.impl.model.file.FileDataModel;
import org.gpfvic.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.gpfvic.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.gpfvic.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.gpfvic.mahout.cf.taste.model.DataModel;
import org.gpfvic.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.gpfvic.mahout.cf.taste.recommender.RecommendedItem;
import org.gpfvic.mahout.cf.taste.recommender.Recommender;
import org.gpfvic.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by afelo on 2016/4/28.
 */
public class SimpleRec {

    public void usage(){
        System.out.println("Usage: XXX <inputfile> <outputfile>");
        System.exit(1);
    }

    public static void printAndExit(String str){
        System.out.println(str);
        System.exit(1);
    }

    public static void readHDFS() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path inFile = new Path("/user/admin/data/ua.base.txt");
        Path outFile = new Path("/user/admin/data/out.txt");

       if(!fs.exists(inFile))
           printAndExit("Input file not found");

        FSDataInputStream in = fs.open(inFile);
        FSDataOutputStream out = fs.create(outFile);
        byte buffer[] = new byte[256];

        try{
            int bytesRead = 0;
            while((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
        }catch(IOException e){
            System.out.println("Error while copying file");
        }finally {
            in.close();
            out.close();
        }




    }

    public static void main(String args[]) throws Exception {

        DataModel model = new FileDataModel(new File("data/ua.base.txt"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.5, similarity,model);

        Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

        List<RecommendedItem> recommendations = recommender.recommend(2,5);

        for(RecommendedItem recommendation: recommendations){
            System.out.println(recommendation);
        }

        readHDFS();
    }

}
