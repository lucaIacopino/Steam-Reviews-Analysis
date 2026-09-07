# Environment Setup — Hadoop, Spark, MongoDB (Ubuntu, single node)

This guide documents how to set up a pseudo-distributed Hadoop cluster,
Spark and MongoDB on a native Ubuntu machine, used for this project.
Every block below can be copy-pasted directly into the terminal.

## 1. Java

```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version
```

## 2. Passwordless SSH to localhost

```bash
sudo apt install ssh -y
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost exit
```

The first connection asks you to confirm the host fingerprint. If it
prompts interactively, type `yes` and press enter. This only happens once.

## 3. Hadoop (pseudo-distributed, single node)

Download and install:

```bash
cd ~/Downloads
wget https://archive.apache.org/dist/hadoop/core/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

Add environment variables to `~/.bashrc` and reload it:

```bash
cat >> ~/.bashrc << 'EOF'

# --- Hadoop environment ---
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
EOF
source ~/.bashrc
```

Write the 4 configuration files (this overwrites them completely with a
valid, minimal single-node config):

```bash
sudo tee $HADOOP_HOME/etc/hadoop/core-site.xml > /dev/null << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_HOME/etc/hadoop/hdfs-site.xml > /dev/null << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_HOME/etc/hadoop/mapred-site.xml > /dev/null << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

sudo tee $HADOOP_HOME/etc/hadoop/yarn-site.xml > /dev/null << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF
```

Set `JAVA_HOME` inside `hadoop-env.sh`:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' | sudo tee -a $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

Format the namenode (**only the first time you ever run this**) and start
the cluster:

```bash
hdfs namenode -format -force
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
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzvf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /usr/local/spark

cat >> ~/.bashrc << 'EOF'

# --- Spark environment ---
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
EOF
source ~/.bashrc
```

Inside the project virtualenv:

```bash
pip install pyspark==3.5.1
```

## 5. MongoDB

Note: MongoDB 7.0 does not officially support Ubuntu 24.04 ("noble"). Use
MongoDB 8.0 instead, which does:

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
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

### Known issue: kernel 6.19+ incompatibility

On systems with a very recent Linux kernel (6.19 or newer), `mongod` may
fail to start with:

```
MongoDB cannot start: Linux kernel versions 6.19 and newer has a known
incompatibility with this version of MongoDB.
```

This is a known conflict between the TCMalloc allocator bundled in
MongoDB 8.x and the kernel's `rseq` handling (see
[SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)). Workaround:

```bash
sudo systemctl edit mongod
```

Add this override:

```ini
[Service]
Environment="GLIBC_TUNABLES=glibc.pthread.rseq=1"
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl restart mongod
sudo systemctl status mongod
```

## 6. Verification checklist

```bash
jps
```
Should show: `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`.

- HDFS web UI reachable at `http://localhost:9870`
- YARN web UI reachable at `http://localhost:8088`

```bash
mongosh --eval "db.version()"
```
Should print the MongoDB version without errors.

```bash
pyspark --version
```
Should print the Spark version without errors.
