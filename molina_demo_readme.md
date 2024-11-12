# Setup Flink Cluster in Session Mode
### 1. Clone the Repository

First, clone the repository where the Docker Compose file is located:

```bash
git clone <repository-url>
cd <repository-directory>
```
From here on, the cloned repository directory would be caled as '<Cloned_Repo_Dir>'.

### 2. SQL Initialization Scripts

Prepare the required Initialization SQL scripts required. This would be used to initialize the SQL CLI Shell.
The initialization scripts are prepared and placed under,

```bash
cat <Cloned_Repo_Dir>/flink-sql-runner/sql-scripts/initialize-data-ingestion-to-adls.sql
```
Add any new additional DDLs required for the Shell to start up.

### 3. Build and prepare the Docker image

Build the repo using Maven and get the Docker image ready.

```bash
cd <Cloned_Repo_Dir>/flink-sql-runner
mvn clean package
docker build . -t flink-sql-runner:latest
```

### 4. Launch the Flink Cluster in Session mode.

You need to have Kubernetes cluster running with CP Flink Operator setup (https://docs.confluent.io/platform/current/flink/get-started.html#step-1-install-af-cp-long)

```bash
cd <Cloned_Repo_Dir>/flink-deployments/flink-sql-jobs
kubectl apply -f flink_cluster_session_mode.yaml
kubectl get pods
```

# Additional K8s Roles for Service Account

You need to provide additional roles to the K8s service account with which the CLI shell is going to get started ('flink')

```bash
cd <Cloned_Repo_Dir>/flink-deployments/flink-sql-jobs
kubectl apply -f K8s_Role.yaml
kubectl get Roles
```

# Launch FLINK SQL CLI in Embedded mode

Launch the Flink SQL CLI shell - an interactive way of submitting the Jobs

```bash
kubeclt exec -it <jobmanager pod> bash
cd /opt/flink/bin
./sql-client.sh -j ../usrlib/sql-runner.jar -i ../usrlib/sql-scripts/initialize-data-ingestion-to-adls.sql
```

# Submit Flink Jobs

```bash
FLINK SQL > INSERT INTO raw_claim_diagnosis_delta_table SELECT claim_id, member_id, diagnosis_code, diagnosis_description, diagnosis_date, lab_results, event_time FROM input_claim_diagnosis;
```
