# FlinkRetailUseCase
Repo to hold the usecases for e-commerce - Shoes inventory and purchases


1. cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase
2. docker compose up -d
3. Spin up 8 connectors through C3 @ http://localhost:9021
4. Validate in Mongo DB @ http://localhost:8082/api_prod_db
5. docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.ShoesInventoryAnalysis /opt/flink/jobs/inventory-analysis-1.0-SNAPSHOT.jar

Verify Mongo DB collection count:
-----
mongosh "mongodb://localhost:27017/api_prod_db" --username api_user --password api1234 --authenticationDatabase api_prod_db
show collections;
db.shoes.countDocuments();
db.shoe_orders.countDocuments();

Find how many shoe orders are placed for a shoe id:
-----
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_info"
    }
  },
  {
    $match: {
      shoe_info: { $ne: [] }
    }
  },
  {
    $group: {
      _id: "$product_id",
      totalOrders: { $sum: 1 }
    }
  }
]).toArray()


Find how many shoe orders are placed for a shoe id - count
---
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_info"
    }
  },
  {
    $match: {
      shoe_info: { $ne: [] }
    }
  },
  {
    $group: {
      _id: "$product_id",
      totalOrders: { $sum: 1 }
    }
  },
  {
    $count: "totalRecords"
  }
])

Find shoes whose totalOrders are more than 5:
---
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_info"
    }
  },
  {
    $match: {
      shoe_info: { $ne: [] }
    }
  },
  {
    $group: {
      _id: "$product_id",
      totalOrders: { $sum: 1 }
    }
  },
  {
    $match: {
      totalOrders: { $gt: 5 }  
    }
  }
]).toArray()


Find shoes whose totalOrders are more than 5: - count
----
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_info"
    }
  },
  {
    $match: {
      shoe_info: { $ne: [] }
    }
  },
  {
    $group: {
      _id: "$product_id",
      totalOrders: { $sum: 1 }
    }
  },
  {
    $match: {
      totalOrders: { $gt: 5 }  
    }
  },
  {
    $count: "totalRecords"
  }
])


db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_info"
    }
  },
  {
    $match: {
      shoe_info: { $ne: [] }
    }
  },
  {
    $count: "totalRecords"
  }
])

Find records whose quantity is less thatn threshold
---
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_details"
    }
  },
  {
    $match: { "shoe_details": { $ne: [] } } // only orders that have a matching shoe
  },
  {
    $group: {
      _id: "$product_id",      // group by product_id
      totalOrders: { $sum: 1 } // count the number of orders for each shoe
    }
  },
  {
    $lookup: {
      from: "shoes",
      localField: "_id",
      foreignField: "id",
      as: "shoe_details"
    }
  },
  {
    $project: {
      product_id: "$_id",
      totalOrders: 1,
      original_quantity: { $toInt: { $arrayElemAt: ["$shoe_details.quantity", 0] } }, // convert quantity to int
      updated_quantity: {
        $subtract: [{ $toInt: { $arrayElemAt: ["$shoe_details.quantity", 0] } }, "$totalOrders"]
      }
    }
  },
  {
    $match: { updated_quantity: { $lt: 8999995 } } // filter for shoes with updated quantity less than 8999995
  }
]).forEach(function(result) {
  print("Product ID:", result.product_id);
  print("Total Orders:", result.totalOrders);
  print("Original Quantity:", result.original_quantity);
  print("Updated Quantity:", result.updated_quantity);
  print("-------------------------------");
});

Find records whose quantity is less thatn threshold - Count
-----
db.shoe_orders.aggregate([
  {
    $lookup: {
      from: "shoes",
      localField: "product_id",
      foreignField: "id",
      as: "shoe_details"
    }
  },
  {
    $match: { "shoe_details": { $ne: [] } } // only orders that have a matching shoe
  },
  {
    $group: {
      _id: "$product_id",      // group by product_id
      totalOrders: { $sum: 1 } // count the number of orders for each shoe
    }
  },
  {
    $lookup: {
      from: "shoes",
      localField: "_id",
      foreignField: "id",
      as: "shoe_details"
    }
  },
  {
    $project: {
      product_id: "$_id",
      totalOrders: 1,
      original_quantity: { $toInt: { $arrayElemAt: ["$shoe_details.quantity", 0] } }, // convert quantity to int
      updated_quantity: {
        $subtract: [{ $toInt: { $arrayElemAt: ["$shoe_details.quantity", 0] } }, "$totalOrders"]
      }
    }
  },
  {
    $match: { updated_quantity: { $lt: 8999995 } } // filter for shoes with updated quantity less than 8999995
  },
  {
    $count: "totalMatchingShoes" // return the total count of matching shoes
  }
])

=============
Order Analysis
---
UC1: From a particulat state, how many orders have been placed by distinct customers
UC2: How many orders of each product has been placed every hour
UC3: Sale Values of products sold every past hour in an interval of 15 mins

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.CustomerOrderDemographics /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.OrderCountEveryDayAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.AverageSalePriceEveryHourAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar


=========
Marketing Analysis
---
UC1: Number of views of each product over last hour in an interval of 15 mins along with expansion of product details
UC2: Pick the top viewed product for every user over last hour in an interval of 15 mins - erich the product and user metadata

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.ViewsPerProductAnalysis /opt/flink/jobs/marketing-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.TopViewedProductByUserAnalysis /opt/flink/jobs/marketing-analysis-1.0-SNAPSHOT.jar