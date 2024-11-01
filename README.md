# FlinkRetailUseCase - Under Construction
Repo to hold the usecases for e-commerce - Shoes inventory and purchases


1. cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase
2. docker compose up -d
3. Spin up 8 connectors through C3 @ http://localhost:9021
4. Validate in Mongo DB @ http://localhost:8082/api_prod_db
5. docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.ShoesInventoryAnalysis /opt/flink/jobs/inventory-analysis-1.0-SNAPSHOT.jar

Verify Mongo DB collection count:
-----
mongosh "mongodb://192.168.0.239:27017/api_prod_db" --username api_user --password api1234 --authenticationDatabase api_prod_db
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
      totalOrders: { $gt: 4 }  
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
UC2: How many orders of each product has been placed every day - Pending
UC3: Sale Values of products sold every past hour in an interval of 15 mins

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.CustomerOrderDemographics /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.OrderCountEveryDayAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.TotalSalePriceEveryHourAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar


=========
Marketing Analysis
---
UC1: Number of views of each product over last hour in an interval of 15 mins along with expansion of product details - Pending
UC2: Pick the top viewed product for every user over last hour in an interval of 15 mins - erich the product and user metadata

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.ViewsPerProductAnalysis /opt/flink/jobs/marketing-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.TopViewedProductByUserAnalysis /opt/flink/jobs/marketing-analysis-1.0-SNAPSHOT.jar


=======
Watchlist  - CEP
---
UC1: Trigger alert when an expected event doesnt occur - User views and adds the product to cart but doesnt place the order within one day of adding item to the cart - Pending

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.TriggerEventsForNullCase /opt/flink/jobs/watchlist-trigger-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.CDCMongoSource /opt/flink/jobs/watchlist-trigger-1.0-SNAPSHOT.jar



=======
Build and publish docker images
---
cd inventory-analysis
You should find a file named Dockerfile
docker build -t myusername/myapp:latest .
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/inventory-analysis:1.0.6 .
docker push dsasidaren/inventory-analysis:1.0.6

cd order-analysis
You should find a file named Dockerfile
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/order-analysis:1.0.5 .
docker push dsasidaren/order-analysis:1.0.5

cd marketing-analysis
You should find a file named Dockerfile
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/marketing-analysis:1.0.4 .
docker push dsasidaren/marketing-analysis:1.0.4

cd watchlist-trigger
You should find a file named Dockerfile
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/watchlist-trigger:1.0.9 .
docker push dsasidaren/watchlist-trigger:1.0.9

Access Docker mongo in K8s
====
Retrieve internal IP
---
ipconfig getifaddr en0
Configure this ip in application properties


Deploy FLink Jobs in Local K8s:
====
cd flink-deployments
kubectl apply -f k8s_pv_pvcs.yaml
kubectl apply -f low_stock_alert.yaml

kubectl port-forward svc/low-stock-alert-rest 8081
Access portal at http://localhost:8081

Verify the Delta lake tables
====
kubectl get pods
Copy the inventory-analysis pod name

kubectl exec -it inventory-analysis-66d4db9d65-tvrzx bash
ls -latr /mount/low_stock_alert/


Logs Search String:
===
Low Stock Alert---->
Customer Order Demographics
Number of Order for a product per day:
Total Sales value of products sold for past hour--->
Total views per product for past hour--->:
Top Viewed Product by Users--->


Deployment Order:
====
(Inventory)
kubectl apply -f low_stock_alert.yaml 

(order analysis)
kubectl apply -f customer_order_demographics.yaml
kubectl apply -f product-demand-analysis-daily.yaml
kubectl apply -f total-sale-value-products-hourly.yaml

(marketing analysis)
kubectl apply -f product-views-recommendations-hourly.yaml
kubectl apply -f top-viewed-product-by-users-hourly.yaml


Copy Delta Tables from K8s Pod to Local for Dashboarding:
=======
kubectl cp <container name>:/mount ./delta-tables


DeltaLake Image spin:
====
docker run -d --name delta_quickstart --rm -it -p 8888-8889:8888-8889 deltaio/delta-docker:latest
docker cp delta_quickstart:/opt/spark/work-dir ./jupyter-notebooks

cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments
docker run -d -v ./jupyter-notebooks/work-dir:/opt/spark/work-dir -v ./delta-tables:/opt/spark/delta-tables --name delta_quickstart --rm -it -p 8888-8889:8888-8889 deltaio/delta-docker:latest


Jupyter Notebook Creation:
======
Navigate to the respective direcotry under 'cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/'
./refresh-dashboard.sh jupyter
Open the logs to check the Notebook URL - docker logs delta_quickstart
launch the notebook @ <URL copied from above>

After the development of notebook, you need to prepare an image for Voila dashboard by following the procedure below:

Voila Dashboard build: (The below 'docker run' are raw commands - This need not be executed. You can use ''./refresh_dashboard.sh voila' instead. But the build commads are to be run.)
======
Low Stock Alert:
-----
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/low-stock-alert
docker build -t low-inventory-alert .
# TO launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name low-inventory-alert-app -p 8866:8866 low-inventory-alert

Customer Order Demographics
-----
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/customer-demographics
docker build -t customer-order-demographics .
# TO launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name customer-order-demographics-app -p 8866:8866 customer-order-demographics

Total Sale Value for past hour
-------
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/total-sale-value
docker build -t total-sale-value .
# TO launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name total-sale-value-app -p 8866:8866 total-sale-value

Top Viewed Products by Users every hour
------
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/top-viewed-products
docker build -t top-viewed-product-personalized .
# TO launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name top-viewed-product-personalized-app -p 8866:8866 top-viewed-product-personalized

Watchlist Trigger
------
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/watchlist-trigger
docker build -t watchlist-trigger .
# To launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name watchlist-trigger-app -p 8866:8866 watchlist-trigger


Demo Execution:
====
1. Low stock Alert

PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/low-stock-alert
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

2. Customer Demogrpahics

PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/customer-demographics
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

3. Total Sale Value for the past hour
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/total-sale-value
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

4. Top Viewed Products by Users during past hour
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/top-viewed-products
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

5. Watchlist trigger
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/watchlist-trigger
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 
