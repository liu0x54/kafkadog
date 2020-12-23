# Kafkadog - a kafka client helps to verificate and troubleshoot the kafka cluster deployment
A kafka client based on the [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
## usage

```
./kafkadog --bootstrap-server YOUR-KAFKA-BOOTSTRAP-SERVER:PORT fastverify --topic testtopic
```
if the result is 
```
success verified, all functions work well
```
means all the verification steps have been passed

## roadmap
more convenient function is planned to help deploy and maintain kafka cluster
- cluster migration
- to be planned