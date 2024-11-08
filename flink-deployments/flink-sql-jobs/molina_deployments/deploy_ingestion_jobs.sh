kubectl delete -f ../ingest_claim_diagnosis_data.yaml
kubectl delete -f ../ingest_claim_procedure_data.yaml
kubectl delete -f ../ingest_claim_provider_data.yaml
kubectl delete -f ../k8s_pv_pvcs.yaml

kubectl apply -f ../k8s_pv_pvcs.yaml
kubectl apply -f ../ingest_claim_diagnosis_data.yaml
kubectl apply -f ../ingest_claim_procedure_data.yaml
kubectl apply -f ../ingest_claim_provider_data.yaml