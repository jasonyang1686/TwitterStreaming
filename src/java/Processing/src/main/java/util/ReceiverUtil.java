package util;

import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.json.JSONObject;

public class ReceiverUtil {
    /*
        Check if the tweet is duplicated, if not then store it into MongoDB.
        For the definition of "duplicated", I assume that means the tweet with exact
        same user id and created time.
     */
    public synchronized void saveDB(String tweets, MongoCollection<Document>collection){
        /* flatten json */
        String flattenTweets = new JsonFlattener(tweets).withFlattenMode(FlattenMode.MONGODB).withSeparator('*').flatten();
        JSONObject jo = new JSONObject(flattenTweets);
        String createAt = (String)jo.get("created_at");
        String id = String.valueOf(jo.get("id"));

        /* create new id, by default mongodb will generate its own _id */
        String newId = id+createAt.replaceAll(" ","");
        /*
            based on the requirement we only need to store unique record.
            check if this record is existed in Mongodb, if not then store it in the database
         */
        UpdateOptions options = new UpdateOptions();
        Document document = new Document();
        document.append("$set", new Document(BasicDBObject.parse(flattenTweets)).append("_id",newId));
        collection.updateOne(Filters.eq("_id",newId),document,options.upsert(true));
        /* print out the count of unique records after keywords filtering */
        System.out.println("Unique Count: "+collection.estimatedDocumentCount());
    }
}
