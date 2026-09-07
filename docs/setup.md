# Environment Setup — Hadoop, Spark, MongoDB (Ubuntu, single node)

This guide documents how to set up a pseudo-distributed Hadoop cluster,
Spark and MongoDB on a native Ubuntu machine, used for this project.

## 1. Java

Hadoop and Spark require Java.

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version
```

## 2. Passwordless SSH to localhost

Hadoop needs to SSH into localhost to start its daemons.

```bash
sudo apt install ssh -y
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost   # should connect without asking for a password, then 'exit'
```

The first connection will ask you to confirm the host fingerprint (type
`yes`). This is normal and only happens once.

## 3. Hadoop (pseudo-distributed, single node)

```bash
cd ~/Downloads
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

Add to the end of `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
```

Then:

```bash
source ~/.bashrc
```

Configure these 4 files inside `$HADOOP_HOME/etc/hadoop/`:

**`core-site.xml`** (inside `<configuration>`):

```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://localhost:9000</value>
</property>
```

**`hdfs-site.xml`**:

```xml
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
```

**`mapred-site.xml`**:

```xml
<property>
  <name>mapreduce.framework.name</name>
  <value>yarn</value>
</property>
```

**`yarn-site.xml`**:

```xml
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
```

Also edit `hadoop-env.sh` and add:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Format the namenode (**only the first time**) and start the cluster:

```bash
hdfs namenode -format
start-dfs.sh
start-yarn.sh
jps
```

`jps` should list: `NameNode`, `DataNode`, `SecondaryNameNode`,
`ResourceManager`, `NodeManager`.

Create the user's HDFS home folder:

```bash
hdfs dfs -mkdir -p /user/$(whoami)
```

## 4. Spark

```bash
cd ~/Downloads
wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzvf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /usr/local/spark
```

Add to `~/.bashrc`:

```bash
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```

```bash
source ~/.bashrc
```

Inside the project virtualenv:

```bash
pip install pyspark==3.5.1
```

## 5. MongoDB

```bash
curl -fsSL https://pgp.mongodb.com/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt update
sudo apt install -y mongodb-org
sudo systemctl start mongod
sudo systemctl enable mongod
sudo systemctl status mongod
```

Inside the project virtualenv:

```bash
pip install pymongo
```

## 6. Verification checklist

- `jps` shows all 5 Hadoop processes (NameNode, DataNode,
  SecondaryNameNode, ResourceManager, NodeManager)
- HDFS web UI reachable at `http://localhost:9870`
- YARN web UI reachable at `http://localhost:8088`
- `mongosh` connects to the Mongo shell without errors
- `pyspark` opens the Spark shell without errors (then `exit()`)
