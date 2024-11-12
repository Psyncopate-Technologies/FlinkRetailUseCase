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
UC2: How many orders of each product has been placed every day 
UC3: Sale Values of products sold every past hour in an interval of 15 mins

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.CustomerOrderDemographics /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.OrderCountEveryDayAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar

docker exec flink-jobmanager flink run -Dtaskmanager.heap.size=2048m -c org.psyncopate.flink.TotalSalePriceEveryHourAnalysis /opt/flink/jobs/order-analysis-1.0-SNAPSHOT.jar


=========
Marketing Analysis
---
UC1: Number of views of each product over last hour in an interval of 15 mins along with expansion of product details - Determine the trending products
UC2: Pick the top viewed product for every user using Session window - erich the product and user metadata

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
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/inventory-analysis:1.0.10 .
docker push dsasidaren/inventory-analysis:1.0.10

cd order-analysis
You should find a file named Dockerfile
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/order-analysis:1.0.12 .
docker push dsasidaren/order-analysis:1.0.12

cd marketing-analysis
You should find a file named Dockerfile
Build Multiplatform images: ENsure to use containerd to pull and push images - a docker setting - by default it would be classic
docker buildx build --platform linux/amd64,linux/arm64 -t dsasidaren/marketing-analysis:1.0.9 .
docker push dsasidaren/marketing-analysis:1.0.9

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

Top Viewed Products by Users
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

Product Demand on daily basis
-----
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/product-demand-analysis
docker build -t product-demand-daily .
# To launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name product-demand-daily-app -p 8866:8866 product-demand-daily

Product Views Trend for past hour Analysis
------
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/product-trend-analysis
docker build -t product-views-trend .
# To launch jupyter server and develop notebooks
docker run -d -v ./delta-tables:/opt/spark/delta-tables --name product-views-trend-app -p 8866:8866 product-views-trend

Demo Execution:
====
1. Low stock Alert - Inventory Analytics

PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/low-stock-alert
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

2. Customer Demogrpahics - Order Analytics

PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/customer-demographics
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

3. Total Sale Value for the past hour - Order Analytics - Tumbling Window
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/total-sale-value
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

4. Top Viewed Products by Users during past hour - Marketing Analytics - Sliding Window
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/top-viewed-products
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

5. Watchlist trigger - CEP
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/watchlist-trigger
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

6. Product Demand on daily basis - Order Analytics - Session Window
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/product-demand-daily
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 

7. Product Trending based on views - hourly - Sliding Window
PPT - Usecase explanation brief
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/product-trend-analysis
./refresh-dashboard.sh voila
http://localhost:8866
./deploy-flink-job.sh 
wait until the job is completed
./refresh-dashboard.sh refresh
http://localhost:8866
./undeploy-flink-job.sh 


# Run Flink SQL Jobs using CP Flink Operator through Table API - Application Mode of Deployment:

Ensure to have your sql files for your job placed under
/Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/sql-scripts

cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner
mvn clean package
docker build . -t flink-sql-runner:latest

Create a K8s Flink Deployment CR under '/Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs'
ex: ingest_claim_diagnosis_data.yaml

Ensure to replace the value of <AZ STORAGE_ACCOUNT ACCESS KEY> if you choose to use abfs filesystem (for ADLS integration).

Navigate to cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs/molina_deployments/deploy_ingestion_jobs.sh

Run the commands one after the other.

If you have a new CR that needs to be submitted, add it to  deploy_ingestion_jobs.sh file and run them as well.

cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
kubectl apply -f <CR_name>.yaml

To use ABFS, ensure to create a config map holding hadoop configurations.
----
cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/configs
kubectl create configmap core-site-config --from-file=core-site.xml

The configmap name created here should be provided in the CR.yaml file under the property - 'kubernetes.hadoop.conf.config-map.name'


Interactive Flink SQL CLI Shell using CP Flink - Try out commands:
-----
./sql-client.sh -j ../usrlib/delta-flink-3.2.1.jar -j ../usrlib/delta-standalone_2.12-3.2.1.jar -j ../usrlib/flink-csv-1.19.0.jar -j ../usrlib/flink-parquet-1.19.0.jar -j ../opt/flink-azure-fs-hadoop-1.19.1.jar -j ../usrlib/delta-storage-3.2.1.jar

./sql-client.sh -j ../usrlib/sql-runner.jar -j ../lib/flink-azure-fs-hadoop-1.19.1.jar -j ../usrlib/hadoop-azure-datalake-3.3.2.jar -j ../usrlib/hadoop-azure-3.3.2.jar

----------
./sql-client.sh -l ../usrlib -j ../opt/flink-azure-fs-hadoop-1.19.1.jar  - Use this if you want to spin sql cli shell in embedded mode (a new jobmanager will be spun)

----------
./sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost (use this if you want to launch the cli connecting to a running job manager)
curl http://localhost:8083/v1/info

./sql-client.sh gateway --endpoint localhost:8083

# FINAL Notes for SQL:

# Run Flink SQL Jobs using CP Flink Operator through Interactive SQL CLI Shell- Session Mode of Deployment:
----
0. Create a COnfig Map holding hadoop configs for the ADLS filesystem
    cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/configs
    kubectl create configmap core-site-config --from-file=core-site.xml

1. Start flink cluster in Session mode
      * Navigate to cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs/flink_cluster_session_mode.yaml
      * kubectl apply -f flink_cluster_session_mode.yaml

   This would start the flink cluster in session mode. 
   kubectl get pods - This will have just the JobManager

2. Provide additional K8s Rolebinding to the service account (created by CP Flink K8s Operator)
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl apply -f K8s_Role.yaml

3. (a) Use Flink SQL CLI to submit jobs - embedded mode
      * Login to the Flink Cluster Job manager Pod (kubeclt exec -it <jobmanager pod> bash)
      * cd /opt/flink/bin
      * ./sql-client.sh -j ../usrlib/sql-runner.jar

    This launches the SQL CLI Shell through which jobs can be submitted.
    Run the desired queries.(from cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/sql-scripts)

3. (b)  Use Flink SQL CLI to submit jobs - gateway mode - yet to be tested
      * Login to the Pod (kubeclt exec -it <jobmanager pod> bash)
      * cd /opt/flink/bin
      * ./sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl apply -f sqlgateway_svc.yaml
      * kubectl port-forward svc/"$SERVICE_NAME" 8083
    
    From your local machine, you need to have flink distribution downloaded and unpacked.
      * Navigate to bin/ directory
      * ./sql-client.sh gateway --endpoint localhost:8083
    
    This launches the SQL CLI Shell through which jobs can be submitted.
    Run the desired queries.(from cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/sql-scripts) -- This is not Tested yet (may require some dependencies to be loaded in classpath)

3. (c)  Use Flink REST API to submit jobs - yet to be tested
      * Login to the Pod (kubeclt exec -it <jobmanager pod> bash)
      * cd /opt/flink/bin
      * ./sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl apply -f sqlgateway_svc.yaml
      * kubectl port-forward svc/"$SERVICE_NAME" 8083

    From your local machine, launch postman
      * Query 1 - Create a session
            curl --location 'http://localhost:8083/sessions' \
                  --header 'Content-Type: application/json' \
                  --data '{
                      "properties": {
                          "execution.runtime-mode": "streaming"
                      }
                  }'
      * Query 2 - Check Session handle details
            curl --location 'http://localhost:8083/sessions/<Session handle ID from pervious response>'

      * Query 3 - Submit SQLs
            curl --location 'http://localhost:8083/sessions/d713c6b5-207f-4d45-93a0-706d6b975c59/statements' \
                --header 'Content-Type: application/json' \
                --data '{
                    "statement": "SHOW CATALOGS;"
                }'
      * Query 4 - View Response
            curl --location 'http://localhost:8083/sessions/d713c6b5-207f-4d45-93a0-706d6b975c59/operations/b8e3d81e-975b-4c8c-9026-d72bb715f56f/result/0?rowFormat=JSON'
    

# Refined1 - Run Flink SQL Jobs using CP Flink Operator through Interactive SQL CLI Shell- Session Mode with Embedded CLI shell:
------
1. Prepare the SQL CLI Shell initialization scripts in a SQL file and place it under
    /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner/sql-scripts
    ex: initialize-data-ingestion-to-adls.sql
2. cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-runner
   mvn clean package
   docker build . -t flink-sql-runner:latest
3. Run Flink Cluster in Session Mode using CP FLink Operator
      * Navigate to cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs/flink_cluster_session_mode.yaml
      * kubectl apply -f flink_cluster_session_mode.yaml
   This would start the flink cluster in session mode. 
   kubectl get pods - This will have just the JobManager

4. Provide additional K8s Rolebinding to the service account (created by CP Flink K8s Operator)
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl apply -f K8s_Role.yaml

5. Use Flink SQL CLI to submit jobs - embedded mode
      * Login to the Flink Cluster Job manager Pod (kubeclt exec -it <jobmanager pod> bash)
      * cd /opt/flink/bin
      * ./sql-client.sh -j ../usrlib/sql-runner.jar -i ../usrlib/sql-scripts/initialize-data-ingestion-to-adls.sql

    This launches the SQL CLI Shell through which jobs can be submitted.

6. Now an interactive Shell prompt is opened from where you can submit INSERT/SELECT Queries which will spin pu a taskamanger
    ex: INSERT INTO raw_claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM input_claim_diagnosis;


# Refined1 - Run Flink SQL Jobs Remotely through Interactive SQL CLI Shell- Session Mode with Gateway CLI shell:
------
1. Prepare the SQL CLI Shell initialization scripts in a SQL file and place it under
    /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-cli
    ex: initialize-data-ingestion-to-adls.sql

2. Ensure to have all job specific dependencies in a directory with in /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-cli

3. Build the image
    cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-sql-cli
    docker build . -t flink-sql-custom-cli:latest

4. Prepare K8s manifest to deploy the image  - this starts up Flink Cluster in Session mode
    cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
    ex: flink_cluster_session_gateway.yaml

5. Deploy the FLink CLuster
    cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
    kubectl apply -f flink_cluster_session_gateway.yaml

6. Deploy the Flink rest service for the SQL gateway of Flink cluster
    cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
    kubectl apply -f sqlgateway_svc_gateway.yaml (Change the labels and selector if required)

7. Provide additional K8s Rolebinding to the service account (created by CP Flink K8s Operator)
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl apply -f K8s_Role.yaml

5. Use Flink SQL CLI to submit jobs - gateway mode
    Login to the Pod (kubeclt exec -it <jobmanager pod> bash)
      * cd /opt/flink/bin
      * ./sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
      * cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase/flink-deployments/flink-sql-jobs
      * kubectl port-forward svc/"$SERVICE_NAME" 8083 - Pick the service name from Step #6

9. From local, where you have the local distribution of flink available, ensure to have all the required libs in lib folder.
    From the bin dir, launch the SQL CLI through the gateway
        ./sql-client.sh gateway --endpoint localhost:8083 -i ../initialize-data-ingestion-to-adls.sql (this is the init sql scripts prepared in step #1)

10. Now an interactive Shell prompt is opened from where you can submit INSERT/SELECT Queries which will spin pu a taskamanger
    ex: INSERT INTO raw_claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM input_claim_diagnosis;

