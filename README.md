# FlinkRetailUseCase
Repo to hold the usecases for e-commerce - Shoes inventory and purchases


1. cd /Users/sasidarendinakaran/Documents/Demos/FlinkRetailUseCase
2. docker compose up -d
3. Spin up 8 connectors through C3 @ http://localhost:9021
4. Validate in Mongo DB @ http://localhost:8082/api_prod_db
5. docker exec flink-jobmanager flink run -c org.psyncopate.flink.ShoesInventoryAnalysis /opt/flink/jobs/inventory-analysis-1.0-SNAPSHOT.jar