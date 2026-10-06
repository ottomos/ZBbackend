#Nasil kosarim:

#Node.js (for the backend and frontend)
#Python 3 (for the Kafka producer simulator)
#Kafka (Apache Kafka running locally, or use Docker)

pip install kafka-python

#node.js indir kur

#hem PC_Code icinde, hem de kafka-nextjs-dashboard'da 
npm install

#kafka start (bunu baska turlu yapiyorsaniz ok)
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.4.9
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  wurstmeister/kafka:2.12-2.2.1

#bu bir terminalde kossun
python KafkaProducerSimulator.py

#yeni terminalde, test etmek icin: 
python KT_gui_v24.py

#multi kafka calistirmak
docker compose -f docker-compose-multi-kafka.yml up -d

#yeni terminalde, start-back-end
node kafka-api.js

#yeni terminalde, start-front-end
cd kafka-nextjs-dashboard
npm run dev

#mail gondermek icin env path ayarlanacak
#cmd line
set ENV_PATH=.env.internal
#powershell
$env:ENV_PATH=".env.internal"

#default user interface
http://localhost:3000

#database yaratma komutu:
DATABASE_URL="file:./dev.db"; npx prisma generate (linux)
set DATABASE_URL=file:./dev.db && npx prisma generate (windows)

#farkli portta kosmak icin, ornek 3001
npx next dev --port 3001

#dev olarak local host disinda kosabilmek icin:
#asagidaki ornek localhost:3001'de gorunur
npx next dev --no-lint --port 3001 --hostname 0.0.0.0

#ms sql kullanmak icin once init etmek gerekiyor:
node init-msdb.js
#bu mssql db calistiracak:
node msdb-api.js

#deploy etmek icin asagidakileri takip edin:
#deployment build
#asagidaki ornek localhost:3001'de ve http://<IP adresimiz>:3001'de gorulur.
cd kafka-nextjs-dashboard
npx next build --no-lint
npx next start --port 3001 --hostname 0.0.0.0

db silmeden tablolari eklemek icin:

CREATE TABLE audit_logs (
  id NVARCHAR(50) PRIMARY KEY,
  userId NVARCHAR(50),
  action NVARCHAR(50) NOT NULL,
  tableName NVARCHAR(50) NOT NULL,
  recordId NVARCHAR(50) NOT NULL,
  changedFields NVARCHAR(MAX),
  timestamp DATETIME DEFAULT GETDATE(),
  ipAddress NVARCHAR(50),
  userAgent NVARCHAR(255)
);

Imaji baska bir makinede calistirmak icin (linux imaj-linux makine)

#BUILD IMAGE (change API URLs to match your infrastructure)
cd kafka-nextjs-dashboard
docker build \
  --build-arg NEXT_PUBLIC_API_URL=http://your-server:4000/api \
  --build-arg NEXT_PUBLIC_DB_API_URL=http://your-server:5000/api \
  -t nextjs-dashboard-prod .

#RUN IMAGE
docker run -d -p 3000:3000 nextjs-dashboard-prod

#OR if you already have the image tar:
docker load -i nextjs-dashboard-prod.tar
docker run -d -p 3000:3000 nextjs-dashboard-prod
# ============================================================================
# REDIS ENTEGRASYONU
# ============================================================================

# Production icin .env dosyasinda guncelle:
# REDIS_CLUSTER_NODES=10.14.4.89:6379,10.14.4.92:6379,...
# REDIS_PASSWORD=<sifre>
# REDIS_INSTANCE_NAME=dev.boa