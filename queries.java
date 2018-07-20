//*** Nicolas Eldering and Yifan Chen (Team 3)
//*** CSC 643 -- Big Data
//*** 2/21/2018
//*** Assignment 1: Get familiar with Mongodb operation and transfer it to java.

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import com.mongodb.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class queries {
    Mongo m = null;
    DB db;
    String localDbName = "LastFM"; // local database name can be adjusted here

    public void connect() {
        try {
            m = new Mongo("localhost", 27017 );
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    //******************************************************
    //*** Purpose: Find the total number of artists in the database.
    //*** Input: None
    //*** Output: total number of artists
    //******************************************************
    public int countArtists(){
        if(m!=null){
            db=m.getDB(localDbName); //connect to database
            DBCollection collection = db.getCollection("artists");
            return (int)collection.count(); //count() returns a long, cast down to int
        }

        return 0; //if connect fails
    }

    //******************************************************
    //*** Purpose: Find URLs of artists whose names contain the word punk.
    //*** Input: None
    //*** Output: Artists name and their URLs
    //******************************************************
    public void listURLsNameContainPunk(){
        if (m!=null){
            db=m.getDB("lastFMdb");
            DBCollection collection = db.getCollection("artists");

            BasicDBObject query = new BasicDBObject();
            BasicDBObject fields = new BasicDBObject();

            Pattern pattern=Pattern.compile(".*punk.*", Pattern.CASE_INSENSITIVE);

            query.put("name",pattern);
            fields.put("name",1);
            fields.put("url", 1);

            DBCursor cur = collection.find(query, fields);

            while(cur.hasNext()) {
                DBObject cursor = cur.next();
                System.out.println("name: " + cursor.get("name")+"  url: "+cursor.get("url"));
            }
        }
    }

    //******************************************************
    //*** Purpose: Find the most listened artist(s).
    //*** Input: None
    //*** Output: the most listened artists
    //******************************************************
    public void listMostListenedArtist(){
        if (m!=null) {
            db = m.getDB(localDbName); //connect to database
            DBCollection collection = db.getCollection("user_artists");
            List<DBObject> operations=new ArrayList<>(); //array for aggregate

            DBObject group = new BasicDBObject("$group", new BasicDBObject("_id","$artistID").append("total", new BasicDBObject("$sum","$weight")));
            DBObject sort=new BasicDBObject("$sort",new BasicDBObject("total",-1));
            DBObject limit=new BasicDBObject("$limit",1); // so we grab only one (our top artist)
            operations.add(group); //add group operations to our array
            operations.add(sort); // add sort operations to our array
            operations.add(limit); // add limit restriction

            AggregationOutput output=collection.aggregate(operations); //aggregation output becomes an iterable
            Iterable<DBObject> iterable = output.results(); // grab the iterable
            Iterator<DBObject> iterator = iterable.iterator(); // make it so we can go through the results of aggregation

            // select details of the most popular artists from artists collection
            DBCollection collection_artists = db.getCollection("artists");

            BasicDBObject query = new BasicDBObject();
            BasicDBObject fields = new BasicDBObject();

            while (iterator.hasNext()) { //parse the results
                query.put("id",iterator.next().get("_id"));
                fields.put("id",1);
                fields.put("name",1);
                fields.put("url",1);
                fields.put("pictureURL",1);

                DBCursor cursor = collection_artists.find(query, fields);
                while(cursor.hasNext()) { // grab our objects
                    BasicDBObject details = (BasicDBObject) cursor.next();
                    System.out.println("id:"+details.get("id")+" name:"+details.get("name")+" URL:"+details.get("url")+" pictureURL:"+details.get("pictureURL"));
                }
            }
        }
    }

    //******************************************************
    //*** Purpose: Compute the top 10 popular artists by using Mapreducer
    //*** Input: None
    //*** Output: ArtistsID, name, URL and pictureURL
    //******************************************************
    public void listTop10Artists(){
        if (m!=null) {
            db = m.getDB("lastFMdb");
            DBCollection collection = db.getCollection("user_artists");
            String map = "function(){"+
                "emit({artistID:this.artistID},{weight:this.weight});"+
                "};";

            String reduce = "function(key,values){"+
                "var weight=0;"+
                "values.forEach(function(v){"+
                    "weight+=v['weight'];"+
                "});"+
                "return {weight:weight};"+
                "};";

            MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                    "output", MapReduceCommand.OutputType.REPLACE, null);

            MapReduceOutput out = collection.mapReduce(cmd);

            DBCollection resultColl = out.getOutputCollection();

            DBObject sort = new BasicDBObject();
            sort.put("value.weight",-1);

            DBCursor cursor= resultColl.find().sort(sort).limit(10);

            DBCollection collection_artists = db.getCollection("artists");

            while(cursor.hasNext()) {
                BasicDBObject details = (BasicDBObject) cursor.next();
                BasicDBObject query = new BasicDBObject();

                query.put("id",((BasicDBObject)details.get("_id")).get("artistID"));
                DBCursor cur = collection_artists.find(query);

                BasicDBObject specificArtist= (BasicDBObject)cur.next();
                System.out.println("id: "+specificArtist.get("id")+"    name: "+specificArtist.get("name")+"    url: "+specificArtist.get("url")+"    pictureURL: "+specificArtist.get("pictureURL"));
            }
        }
    }

    //******************************************************
    //*** Purpose: Write a mapReducer to find the least used tag.
    //*** Input: None
    //*** Output: find the least used tag
    //******************************************************
    public void listLeastUsedTag(){
        if (m!=null) {
            db = m.getDB(localDbName); // connect to database
            DBCollection collection = db.getCollection("user_taggedartists");
            DBCollection collection_tags = db.getCollection("tags");
            String map = "function(){" + // map function to emit tags
                    "emit({tagID:this.tagID},{count:1});"+
                    "};";

            String reduce = "function(key,values){"+ //reduce to sum our tages
                    "var count=0;"+
                    "values.forEach(function(v){"+
                    "count+=v['count'];"+
                    "});"+

                    "return {count:count};"+
                    "};";

            MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce,
                    "output", MapReduceCommand.OutputType.REPLACE, null); // build command

            MapReduceOutput out = collection.mapReduce(cmd); // run mongodb command and catch

            DBCollection resultColl = out.getOutputCollection(); // collect our result

            DBObject sort = new BasicDBObject();
            sort.put("value.count",1); //sort

            DBCursor cursor= resultColl.find().sort(sort).limit(1); // grab the least used tag

            BasicDBObject query = new BasicDBObject();

            while(cursor.hasNext()) { //list our tag
                query.put("tagID", ((BasicDBObject) cursor.next().get("_id")).get("tagID"));
                DBCursor cur = collection_tags.find(query);
                while (cur.hasNext()) {
                    System.out.println(cur.next());
                }
            }
        }
    }

    //******************************************************
    //*** Purpose: Compute the average number of tags used by each user for all artists. by using Mapreducer
    //*** Input: None
    //*** Output: The average number of tags used by each user for all artists
    //******************************************************
    public void averageTagsbyEachUser(){
        if (m!=null) {
            db = m.getDB("lastFMdb");
            DBCollection collection = db.getCollection("user_taggedartists");

            //calculate the tags' number users used
            String map_tagsNum = "function(){"+
                    "emit({userID:this.userID},{count:1});"+
                "};";

            String reduce_tagsNum="function(key,values){"+
                "var cnt=0;"+
                "values.forEach(function(v){"+
                    "cnt+=v['count'];"+
                "});"+
                "return {count:cnt};"+
                "};";

            MapReduceCommand cmd = new MapReduceCommand(collection, map_tagsNum, reduce_tagsNum,
                    "output", MapReduceCommand.OutputType.REPLACE, null);

            MapReduceOutput out = collection.mapReduce(cmd);

            DBCollection resultColl = out.getOutputCollection();

            DBCursor cursor = resultColl.find();

            ArrayList<Double> tags_number_array=new ArrayList<>();
            ArrayList<String> userID_array=new ArrayList<>();

            while(cursor.hasNext()) {
                DBObject iter=cursor.next();
                tags_number_array.add((double)((BasicDBObject) iter.get("value")).get("count"));
                userID_array.add(((BasicDBObject) iter.get("_id")).get("userID").toString());
            }

            // calculate the artists number the user tagged
            String map_artistsNum_temp="function(){"+
                    "emit({userID:this.userID, artistID:this.artistID},{count:1});"+
                "};";

            String reduce_artistsNum_temp="function(key,values){"+
                    "var cnt=0;"+
                    "values.forEach(function(v){"+
                        "cnt+=v['count'];"+
                    "});"+
                    "return {count:cnt};"+
                "};";

            cmd = new MapReduceCommand(collection, map_artistsNum_temp, reduce_artistsNum_temp,
                    "output", MapReduceCommand.OutputType.REPLACE, null);

            out = collection.mapReduce(cmd);

            DBCollection resultColl_artistsNum_temp = out.getOutputCollection();

            String map_artistsNum="function(){"+
                    "emit({userID:this._id.userID},{count:1});"+
                "};";

            String reduce_artistsNum="function(key,values){"+
                    "var cnt=0;"+
                    "values.forEach(function(v){"+
                        "cnt+=v['count'];"+
                    "});"+
                    "return {count:cnt};"+
                "};";

            cmd = new MapReduceCommand(resultColl_artistsNum_temp, map_artistsNum, reduce_artistsNum,
                    "output", MapReduceCommand.OutputType.REPLACE, null);

            out = resultColl_artistsNum_temp.mapReduce(cmd);

            DBCollection resultColl_artistsNum = out.getOutputCollection();

            cursor = resultColl_artistsNum.find();

            ArrayList<Double> artists_number_array=new ArrayList<>();

            while(cursor.hasNext()) {
                artists_number_array.add((double)((BasicDBObject) cursor.next().get("value")).get("count"));
            }

            ArrayList<Double> average=new ArrayList<>();
            for (int i=0;i<artists_number_array.size();i++){
                average.add((tags_number_array.get(i))/(artists_number_array.get(i)));
                System.out.println("userID: "+userID_array.get(i)+"  average: "+average.get(i));
            }
        }
    }

    public static void main(String[] args) {
        ConnectToMongoDB connectToMongoDB = new ConnectToMongoDB();
        connectToMongoDB.connect();

        //The total number of artists
        System.out.println("The total number of artists is "+connectToMongoDB.countArtists());
        System.out.println();

        //The URLs of artists whose names contain the word "punk"
        System.out.println("The URLs of artists whose names contain the word \"punk\" are ");
        connectToMongoDB.listURLsNameContainPunk();
        System.out.println();

        //The most listened artist
        System.out.println("The most listened artist is ");
        connectToMongoDB.listMostListenedArtist();
        System.out.println();

        //Top 10 popular artists
        System.out.println("Top 10 popular artists are ");
        connectToMongoDB.listTop10Artists();
        System.out.println();

        //The least used tag
        System.out.print("The least used tag is ");
        connectToMongoDB.listLeastUsedTag();
        System.out.println();

        //The average number of tags used by each user for all artists
        System.out.println("The average number of tags used by each user for all artists is: ");
        connectToMongoDB.averageTagsbyEachUser();
    }
}