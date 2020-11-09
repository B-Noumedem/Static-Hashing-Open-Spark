import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class rowdata {
    String ID;
    String title;
	
    public rowdata(String[] params) {
        if (params.length != 2) {
            System.err.println("Chwaya les params:" + Arrays.toString(params) + "\n\tLen=" + params.length);
        }
        ID = params[0];
        title = params[1];
    }

    public rowdata(String __ID, String __title) {
        ID = __ID;
        title = __title;
    }
    

    public void setKey(String key){
        this.ID = key;
    }

    public String getKey(){
        return this.ID;
    }

    public void setValue(String value){
        this.title = value;
    }

    public String getValue(){
        return this.title;
    }

    public void showData() {
        System.out.println("\n\t ID= " + ID + "\n\t FirstName= " + title);
    }

    public static ArrayList<rowdata> readFromCSV(String filename, boolean Headers, String cvsSplitBy) {
        ArrayList<rowdata> list = new ArrayList<>();
        try {
            File myFile = new File(filename);
            Scanner myReader = new Scanner(myFile);
            if (Headers) {
                String __ = myReader.nextLine();
            }
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                if (line.length() == 0) {
                    continue;
                }
                String[] lineSplitted = line.split(cvsSplitBy);
                list.add(new rowdata(lineSplitted[2],lineSplitted[4]));
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return (list);
    }
}