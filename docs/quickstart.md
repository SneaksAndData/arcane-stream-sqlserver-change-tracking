# Quickstart Guide

## Prerequisites

This guide assumes you have the Kubernetes cluster version 1.29 or higher and the Arcane Operator version
0.0.12 or higher installed in your cluster.

This plugin supports only Amazon S3 compatible storages and authentication using AWS access key and secret key.

## Installation

1. Create a S3 bucket in your AWS account.
2. Create an IAM user with the following permissions:
    - `s3:PutObject`

3. Create a Kubernetes secret with the following keys:
```bash
$ kubectl create secret generic s3-secret \
    --from-literal=ARCANE.STREAM.SQLSERVERCHANGETRACKING__AWS_ACCESS_KEY_ID=<access-key-id> \
    --from-literal=ARCANE.STREAM.SQLSERVERCHANGETRACKING__AWS_ENDPOINT_URL=https://s3.<region>.amazonaws.com \
    --from-literal=ARCANE.STREAM.SQLSERVERCHANGETRACKING__AWS_SECRET_ACCESS_KEY=<access-key-secret>
 ``` 

4. Create a Kubernetes secret containing the connection string to the SQL Server database:
```bash
$ kubectl create secret generic db-secret \
    --from-literal=ARCANE_CONNECTIONSTRING=<connection-string>
 ``` 

4. Deploy the Arcane Stream plugin:
```bash
$ helm install arcane-stream-rest-api oci://ghcr.io/sneaksanddata/helm/arcane-stream-sqlserver-change-tracking \
  --version v0.0.4 \
  --set jobTemplateSettings.extraEnvFrom[0].secretRef.name=s3-secret \
  --namespace arcane
```

5. Verify the installation
To verify the plugin installation run the following command:
```bash
$ kubectl get stream-classes --namespace arcane -l app.kubernetes.io/name=arcane-stream-sqlserver-change-tracking
```
It should produce the similar output:
```
NAME                                      PLURAL                  APIGROUPREF                 PHASE
arcane-stream-sqlserver-change-tracking   sql-server-ct-streams   streaming.sneaksanddata.com READY
```

6. Create a stream definition:
```bash
export SINK_LOCATION=s3a://bucket/folder
export TABLE=example

cat <<EOF | envsubst | kubectl apply -f -
apiVersion: streaming.sneaksanddata.com/v1beta1
kind: SqlServerChangeTracking
metadata:
  name: sql-server-change-tracking-stream
  namespace: arcane
spec:
  changeCaptureIntervalSeconds: 15
  commandTimeout: 3600
  connectionStringRef:
    name: db-secret
  groupingIntervalSeconds: 15
  groupsPerFile: 1
  lookBackInterval: 1209600
  rowsPerGroup: 50000
  schema: dbo
  sinkLocation: ${SINK_LOCATION}
  table: ${TABLE}
  jobTemplateRef:
    apiGroup: streaming.sneaksanddata.com
    kind: StreamingJobTemplate
    name:  arcane-stream-sqlserver-change-tracking-standard-job
  backfillJobTemplateRef:
    apiGroup: streaming.sneaksanddata.com
    kind: StreamingJobTemplate
    name: arcane-stream-sqlserver-change-tracking-large-job

EOF
```

6. Soon you should see the Kubernetes Job with name `sql-server-change-tracking-stream` in the `arcane` namespace and the stream definition in the `RELOADING` phase:
```bash
$ kubectl get jobs -n arcane
NAME                              COMPLETIONS   DURATION   AGE
sql-server-change-tracking-stream       0/1           0s         0s

$ kubectl get api-pdas -n arcane 
NAME                                TABLE     SCHEMA   REFRESH INTERVAL   SINK LOCATION       PHASE
sql-server-change-tracking-stream   example   dbo      15                 s3a://bucket/folder RELOADING
```
