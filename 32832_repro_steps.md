# 32832 Repro Recipe
The client code can be found here: https://github.com/colton-aerospike/praxis

Theres a dockerfile and a public image available to make it easier to run. 
I've been testing this in EKS using m5ad.xlarge instances. 
The client can be ran outside of k8s by compiling the code locally (instructions further below)

## Setup EKS environment in GAIA:
 - Go to https://gaia.aerospike.com/eks
 - Press 'New Cluster'
 - I used the following settings but feel free to play around:
    - Cluster Name: colton
    - Min k8s nodes: 5
    - Max k8s nodes: 6
    - Subnets: 1
    - Node Groups: 1
    - Instance Types: m5ad.xlarge
    - The rest are default
 - Once the cluster is up and running enter the docker container using the console script from GAIA 
 		- If you dont already have one setup follow the instructions under "Create/start the EKS container and upload the features file" here: https://gaia.aerospike.com/eks/cheat
 - Deploy the cluster. Easiest option is to use the automated OLM deploy script: https://github.com/colton-aerospike/deploy-olm-ako
 		- On newer gaia-eks.sh environments these scripts are already loaded in the container
 - Run command to deploy Aerospike cluster using OLM: ./setup_olm.sh -n colton -v 1.27 -E -A
 - OPTIONAL - for best results its better to taint and label the nodes used specifically for aerospike. This way we can ensure nothing else is running on the nodes outside of k8s/cloud provider processes.
     - kubectl label node <node_hostname_here> app=aerospike
     - kubectl taint node <node_hostname_here> app=aerospike:NoSchedule
 	 - NOTE: perform the same for the praxis client on a different node (if using settings above this would be the 5th node out of 4 and using 4 nodes for Aerospike DB cluster)
 	 - kubectl label node <node_hostname_here> app=praxis
 	 - kubectl taint node <node_hostname_here> app=praxis:NoSchedule
 - Assuming you have the same taints and labels you can now deploy the below YAML. 
 	=== NOTE === You will need to remove the tolerations and nodeSelector if you did NOT apply labels and taints
 	- kubectl apply -f aerospike_shadow_single_namespace.yaml

## Aerospike Cluster YAML:
```yaml
❯ cat aerospike_shadow_single_namespace_ebs.yaml
apiVersion: asdb.aerospike.com/v1
kind: AerospikeCluster
metadata:
  name: asdb-dev
  namespace: aerospike

spec:
  size: 4
  image: aerospike/aerospike-server-enterprise:6.3.0.5
  storage:
    filesystemVolumePolicy:
      initMethod: deleteFiles
      cascadeDelete: true
    blockVolumePolicy:
      cascadeDelete: true
    volumes:
      - name: workdir
        aerospike:
          path: /opt/aerospike
        source:
          persistentVolume:
            storageClass: gp2
            volumeMode: Filesystem
            size: 1Gi
      - name: test
        aerospike:
          path: /aerospike/dev/xvdf_test
        source:
          persistentVolume:
            storageClass: openebs-device # local nvme drive attached to k8s node
            volumeMode: Block
            size: 100Gi
      - name: aerospike-config-secret
        source:
          secret:
            secretName: aerospike-secret
        aerospike:
          path: /etc/aerospike/secret
      - name: exporter-config
        sidecars:
          - containerName: aerospike-prometheus-exporter
            path: /opt/aerospike-prometheus-exporter/config/
        source:
          configMap:
            name: exporter-config
  podSpec:
    multiPodPerHost: false
    metadata:
      annotations:
    tolerations:
      - key: "app"
        value: "aerospike"
        operator: "Equal"
        effect: "NoSchedule"

    sidecars:
    - name: aerospike-prometheus-exporter
      image: aerospike/aerospike-prometheus-exporter:latest
      command: ["/bin/sh", "-c"]
      args:
        - aerospike-prometheus-exporter --config /opt/aerospike-prometheus-exporter/config/ape.toml
      ports:
        - containerPort: 9145
          name: exporter
      env:
        - name: "AS_AUTH_USER"
          value: "admin"
        - name: "AS_AUTH_PASSWORD"
          value: "admin123"
        - name: "AS_AUTH_MODE"
          value: "internal"
  rackConfig:
    namespaces:
      - test
    racks:
      - id: 1
        zone: us-east-1a
      - id: 2
        zone: us-east-1a
          #- id: 3
          #zone: us-east-1c
  aerospikeAccessControl:
    users:
      - name: admin
        secretName: auth-secret
        roles:
          - sys-admin
          - user-admin
          - read-write
          - sindex-admin
          - read-write-udf
          - udf-admin

  aerospikeConfig:
    service:
      feature-key-file: /etc/aerospike/secret/features.conf
      proto-fd-max: 25000
    security: {}
    logging:
      - name: console
        any: info
      - name: /opt/aerospike/aerospike.log
        any: info
        query: critical
    network:
      service:
        port: 3000
      fabric:
        port: 3001
      heartbeat:
        port: 3002
    namespaces:
      - name: test
        memory-size: 3000000000
        replication-factor: 2
        nsup-period: 120
        storage-engine:
          type: device
          serialize-tomb-raider: false
          cold-start-empty: false
          write-block-size: 524288
          data-in-memory: true
          devices:
            - /aerospike/dev/xvdf_test
```

## Deploy the Praxis client in EKS:
 - Assuming you have the taints and labels added to the node you can deploy the YAML below:
 	- === NOTE === Same as cluster yaml above - REMOVE TOLERATIONS ONLY IF YOU DIDNT APPLY TAINTS AND LABELS

Praxis Deployment YAML:
```yaml
❯ cat praxis.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: praxis
  labels:
    app: praxis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: praxis
  template:
    metadata:
      labels:
        app: praxis
    spec:
      tolerations:
        - key: "app"
          operator: "Equal"
          value: "praxis"
          effect: "NoSchedule"
      nodeSelector:
        app: praxis
      containers:
      - name: praxis
        image: coltonmarkle/praxis:1.4.1
        imagePullPolicy: Always
        env:
          - name: PRAXIS_USER
            value: "admin"
          - name: PRAXIS_PASS
            value: "admin123"
          - name: PRAXIS_HOST
            value: "asdb-dev.aerospike.svc.cluster.local"
          - name: PRAXIS_NAMESPACE
            value: "test"
          - name: "PRAXIS_SETNAME"
            value: "myset"
          - name: PRAXIS_SLEEP_TIMER
            value: "100ms"
          - name: PRAXIS_KEY_COUNT
            value: "50000"
          - name: PRAXIS_CHANNEL_SIZE
            value: "500"
```

These environment values can be tuned, but its likely best to keep as is. In the event you need to increase TPS it may be better to run multiple instances of the client. 
Changing the size from 1 -> 2 will start up a second instance, but be sure to watch where it lands because it may just start on the same node. Resulting in 2 clients on the same kubernetes node which may cause issues.
If needed, the best options to tune are PRAXIS_SLEEP_TIMER and PRAXIS_CHANNEL_SIZE
The sleep timer defines how long to wait before starting the next iteration. Can be specified as 500ms or 2s for example
The channel size is how many transaction to be sent concurrently. I wouldn't go above 1000, or really 700. Having too many goroutines at once can cause high memory, context switching, and bad times. 

To monitor the praxis logs you can run:
 - kubectl logs -naerospike -f deployments/praxis

Here's an example:
```bash
❯ kubectl logs -naerospike -f deployments/praxis
2023/09/19 16:59:20 100ms
2023/09/19 16:59:20 Initializing client and policy.
2023/09/19 16:59:20 Connected!
2023/09/19 16:59:20 Warming up connections.
2023/09/19 16:59:21 Registered UDFs!
2023/09/19 16:59:21 Completed creating all sindexes!
2023/09/19 16:59:21 Starting job.
```

## Running the client without docker:
 - Clone the repo: git clone https://github.com/colton-aerospike/praxis.git
 - Compile: 
 	- for x86_64: GOOS=linux GOARCH=amd64 go build -o /opt/praxis/praxis
 	- for ARM (not tested): GOOS=linux GOARCH=arm64 go build -o /opt/praxis/praxis
 - Run the code: /opt/praxis/praxis -h ${PRAXIS_HOST} -U ${PRAXIS_USER} -P ${PRAXIS_PASS} -k ${PRAXIS_KEY_COUNT} -n ${PRAXIS_NAMESPACE} -s ${PRAXIS_SETNAME} -chan ${PRAXIS_CHANNEL_SIZE} -sT ${PRAXIS_SLEEP_TIMER} -q -iB mapBin -iV 8675309
 - You can use the environment variables in the praxis.yaml above as a guideline.


## Pulling The Logs:
 - The YAML above logs to both console and a file on the k8s node. Its best to grab both. The file will disappear if the node is restarted.
 - Tail logs in real time:
 	- for pod in $(kubectl get pods -naerospike | grep asdb | awk '{print $1}'); do kubectl logs -naerospike -f ${pod} > ${pod}_live_tail.log &;done
 - COPY logs from pod:
 	- for pod in $(kubectl get pods -naerospike | grep asdb | awk '{print $1}'); do kubectl cp -naerospike -caerospike-server $pod:/opt/aerospike/aerospike.log $pod.log ;done
 - Can use sftp from the container/local shell to copy to asftp. 


