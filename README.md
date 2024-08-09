# delay queue by kafka
## 1.description
use the capabilities of kafka sdk to build a delay-queue
## 2.version
 - little biz volume prioritize the branch -- feature-kafka_delay_queue-1.0.1
 - high scalability and high availability high biz volumn prioritize the branch --feature-kafka_delay_queue-2.0.1
### 2.1 version 1.0.1
provides 17 levels of delay queue capabilities,but all levels of delay queue will be created by every instance, event
thus this server boot create multiple instance. maybe cause high cpu usage.
### 2.2 version 2.0.1
provider 17 levels of delay queue capabilities, and all levels of delay queue will be assigned to multiple instances.
so delay queue core boot has load balancing capabilities. otherwise, delay topic unlimited horizontal scalability.