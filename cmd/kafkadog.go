package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var ()

func init() {
	/*
		flag.StringVar(&bootstrap, "bootstrap-server", "", "kafka cluster bootstrap server.")
		flag.StringVar(&topic, "topic", "testtopic", "topic that used to test")
		flag.IntVar(&numPart, "partition-count", 10, "number of partions")
		flag.IntVar(&replicas, "replicas", 3, "number of replicas")
		flag.StringVar(&rsType, "resource-type", "", "topic, broker or consumer group")
		flag.StringVar(&rsName, "resource-name", "", "broker id or topic name or consumer group id")
		flag.BoolVar(&describers, "describe", false, "describe the resource")

		flag.Parse()
		if bootstrap == "" {
			fmt.Println("should input bootstrap server")
			flag.PrintDefaults()
			//os.Exit(1)
		}
		if describers && (rsType == "" || rsName == "") {
			fmt.Println("describe should have resource type and name")
			flag.PrintDefaults()
			//os.Exit(1)

		}
	*/

}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app := &cli.App{
		Name:  "kafkadog",
		Usage: "a kafka client to fast verify the function of kafka cluster",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "bootstrap-server",
				Aliases: []string{"bs"},
				Value:   "localhost:9094",
				Usage:   "bootstrap server of kafka cluster",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "create",
				Aliases: []string{"cr"},
				Usage:   "create a topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},

						Usage: "topic to create",
					},
					&cli.IntFlag{
						Name:    "partitions",
						Aliases: []string{"p"},
						Value:   10,
						Usage:   "the number of partitions",
					},
					&cli.IntFlag{
						Name:    "replicas",
						Aliases: []string{"r"},
						Value:   3,
						Usage:   "the number of replicas",
					},
				},
				Action: func(c *cli.Context) error {
					fmt.Println("created topic: ", c.String("topic"))
					topic := c.String("topic")
					bootstrap := c.String("bootstrap-server")
					numPart := c.Int("partitions")
					replicas := c.Int("replicas")
					ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						panic(err)
					}

					wait, err := time.ParseDuration("10s")
					if err != nil {
						panic("ParseDuration(10s)")
					}
					results, err := ac.CreateTopics(
						ctx,
						[]kafka.TopicSpecification{{
							Topic:             topic,
							NumPartitions:     numPart,
							ReplicationFactor: replicas}},

						kafka.SetAdminOperationTimeout(wait))
					if err != nil {
						fmt.Printf("ERROR: create topic: %v\n", err)
						os.Exit(1)
					}

					for _, result := range results {
						fmt.Printf("%s\n", result)
					}
					ac.Close()
					return nil
				},
			},
			{
				Name:    "delete",
				Aliases: []string{"del"},
				Usage:   "delete a topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},

						Usage: "topic to delete",
					},
				},
				Action: func(c *cli.Context) error {
					topics := []string{c.String("topic")}
					bootstrap := c.String("bootstrap-server")
					ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						panic(err)
					}

					wait, err := time.ParseDuration("10s")
					if err != nil {
						panic("ParseDuration(10s)")
					}
					results, err := ac.DeleteTopics(
						ctx,
						topics,
						kafka.SetAdminOperationTimeout(wait))
					if err != nil {
						fmt.Printf("ERROR: delete topic: %v\n", err)
						os.Exit(1)
					}

					for _, result := range results {
						fmt.Printf("%s\n", result)
					}
					ac.Close()
					return nil
				},
			},
			{
				Name:    "produce",
				Aliases: []string{"p"},
				Usage:   "produce messages to the topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "which topic to produce msg",
					},
					&cli.IntFlag{
						Name:    "partition",
						Aliases: []string{"p"},
						Value:   -1,
						Usage:   "which partition to produce msg, if not set, produced to a random partition",
					},
					&cli.IntFlag{
						Name:    "messagenum",
						Aliases: []string{"m"},
						Value:   20,
						Usage:   "how many messages to send",
					},
				},
				Action: func(c *cli.Context) error {
					bootstrap := c.String("bootstrap-server")

					p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						fmt.Printf("Failed to create producer: %s\n", err)
						os.Exit(1)
					}
					fmt.Printf("Created Producer %v\n", p)
					topic := c.String("topic")
					msgnum := c.Int("messagenum")
					partition := c.Int("partition")
					deliveryChan := make(chan kafka.Event)
					var topicpartition kafka.TopicPartition

					if partition == -1 {
						topicpartition = kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
					} else {
						topicpartition = kafka.TopicPartition{Topic: &topic, Partition: int32(partition)}
					}

					msg := "Greeting from Kafkadog!"
					for i := 0; i < msgnum; i++ {
						value := msg + " NO. " + strconv.Itoa(i)
						err = p.Produce(&kafka.Message{
							TopicPartition: topicpartition,
							Value:          []byte(value),
							Headers:        []kafka.Header{{Key: "Header", Value: []byte("binary")}},
						}, deliveryChan)

						e := <-deliveryChan
						m := e.(*kafka.Message)

						if m.TopicPartition.Error != nil {
							fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
						} else {
							fmt.Printf("Delivered to topic %s [%d] at offset %v\n",
								*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
						}

					}

					close(deliveryChan)
					return nil
				},
			},
			{
				Name:    "consume",
				Aliases: []string{"c"},
				Usage:   "consume messages from the topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "which topic to consume msg",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Value:   "",
						Usage:   "consumer group.id",
					},
					&cli.BoolFlag{
						Name:    "standby",
						Aliases: []string{"s"},
						Value:   false,
						Usage:   "if true, the consumer will standby to wait future msg; if false, the consumer will exit when all msg are consumed",
					},
					&cli.BoolFlag{
						Name:    "print",
						Aliases: []string{"p"},
						Value:   false,
						Usage:   "if true, print the message",
					},
				},
				Action: func(c *cli.Context) error {
					bootstrap := c.String("bootstrap-server")
					groupid := c.String("group")
					topics := []string{c.String("topic")}
					stb := c.Bool("standby")
					prt := c.Bool("print")
					totalmsg := 0

					sigchan := make(chan os.Signal, 1)
					signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

					consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
						"bootstrap.servers":     bootstrap,
						"broker.address.family": "v4",
						"group.id":              groupid,
						"session.timeout.ms":    6000,
						"auto.offset.reset":     "earliest"})

					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
						os.Exit(1)
					}

					fmt.Printf("Created Consumer %v\n", c)

					err = consumer.SubscribeTopics(topics, nil)

					run := true

					for run == true {
						select {
						case sig := <-sigchan:
							fmt.Printf("Caught signal %v: terminating\n", sig)
							run = false
						default:
							ev := consumer.Poll(100)
							if ev == nil {
								continue
							}

							switch e := ev.(type) {
							case *kafka.Message:
								if prt {
									fmt.Printf("%% Message on %s:\n%s\n",
										e.TopicPartition, string(e.Value))
									if e.Headers != nil {
										fmt.Printf("%% Headers: %v\n", e.Headers)
									}
								}

								totalmsg++
							case kafka.Error:

								fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
								if e.Code() == kafka.ErrAllBrokersDown {
									run = false
								}
							default:
								fmt.Printf("Ignored %v\n", e)
								if !stb {
									fmt.Printf("Consumed %d messages!\n", totalmsg)
									consumer.Close()
									return nil
								}
							}
						}
					}

					fmt.Printf("Closing consumer\n")
					fmt.Printf("Consumed %d messages!\n", totalmsg)
					consumer.Close()
					return nil
				},
			},
			{
				Name:    "fastverify",
				Aliases: []string{"fv"},
				Usage:   "fast verify the function of kafka cluster after deployment",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "which topic to consume msg",
					},
					&cli.IntFlag{
						Name:    "partition",
						Aliases: []string{"p"},
						Value:   10,
						Usage:   "how many partition in the topic",
					},
					&cli.IntFlag{
						Name:    "replica",
						Aliases: []string{"r"},
						Value:   3,
						Usage:   "how many replicas in the partition",
					},
					&cli.IntFlag{
						Name:    "msgnum",
						Aliases: []string{"n"},
						Value:   50,
						Usage:   "how many msgs to test",
					},
				},
				Action: func(c *cli.Context) error {
					bootstrap := c.String("bootstrap-server")
					ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						panic(err)
					}
					topic := c.String("topic")
					fmt.Println("created topic: ", topic)

					numPart := c.Int("partition")
					replicas := c.Int("replica")
					numMsg := c.Int("msgnum")

					wait, err := time.ParseDuration("10s")
					if err != nil {
						panic("ParseDuration(10s)")
					}
					results, err := ac.CreateTopics(
						ctx,
						[]kafka.TopicSpecification{{
							Topic:             topic,
							NumPartitions:     numPart,
							ReplicationFactor: replicas}},

						kafka.SetAdminOperationTimeout(wait))
					if err != nil {
						fmt.Printf("ERROR: create topic: %v\n", err)
						os.Exit(1)
					}

					for _, result := range results {
						fmt.Printf("%s\n", result)
					}
					fmt.Printf("start to produce to topic %s \n", topic)
					p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						fmt.Printf("Failed to create producer: %s\n", err)
						os.Exit(1)
					}
					deliveryChan := make(chan kafka.Event)

					msg := "Greeting from Kafkadog!"
					for i := 0; i < numMsg; i++ {
						value := msg + " NO. " + strconv.Itoa(i)
						err = p.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
							Value:          []byte(value),
							Headers:        []kafka.Header{{Key: "Header", Value: []byte("binary")}},
						}, deliveryChan)

						e := <-deliveryChan
						m := e.(*kafka.Message)

						if m.TopicPartition.Error != nil {
							fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
						} else {
							fmt.Printf("Delivered to topic %s [%d] at offset %v\n",
								*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
						}
					}
					fmt.Printf("Produced %d messages!\n", numMsg)
					close(deliveryChan)

					fmt.Printf("start to consume topic %s \n", topic)
					groupid := "group1"
					topics := []string{topic}
					stb := false
					prt := false
					totalmsg := 0

					sigchan := make(chan os.Signal, 1)
					signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

					consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
						"bootstrap.servers":     bootstrap,
						"broker.address.family": "v4",
						"group.id":              groupid,
						"session.timeout.ms":    6000,
						"auto.offset.reset":     "earliest"})

					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
						os.Exit(1)
					}

					//fmt.Printf("Created Consumer %v\n", c)

					err = consumer.SubscribeTopics(topics, nil)

					run := true

					for run == true {
						select {
						case sig := <-sigchan:
							fmt.Printf("Caught signal %v: terminating\n", sig)
							run = false
						default:
							ev := consumer.Poll(100)
							if ev == nil {
								continue
							}

							switch e := ev.(type) {
							case *kafka.Message:
								if prt {
									fmt.Printf("%% Message on %s:\n%s\n",
										e.TopicPartition, string(e.Value))
									if e.Headers != nil {
										fmt.Printf("%% Headers: %v\n", e.Headers)
									}
								}

								totalmsg++
							case kafka.Error:

								fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
								if e.Code() == kafka.ErrAllBrokersDown {
									run = false
								}
							default:
								//fmt.Printf("Ignored %v\n", e)
								if !stb {
									fmt.Printf("Consumed %d messages!\n", totalmsg)
									for i := 0; i < numPart; i++ {
										showPartitionOffset(consumer, topic, i)
									}
									consumer.Close()
									if totalmsg != numMsg {
										fmt.Printf("messages produced does not match the message consumed!\n")
										os.Exit(1)
									} else {
										run = false
									}
								}
							}
						}
					}

					results, err = ac.DeleteTopics(
						ctx,
						topics,
						kafka.SetAdminOperationTimeout(wait))
					if err != nil {
						fmt.Printf("ERROR: delete topic: %v\n", err)
						os.Exit(1)
					}

					for _, result := range results {
						fmt.Printf("%s deleted\n", result)
					}
					fmt.Printf("success verified, all functions work well\n")
					ac.Close()
					os.Exit(0)
					return nil
				},
			},
			{
				Name:    "describe",
				Aliases: []string{"dc"},
				Usage:   "describe a resource",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "type",
						Aliases: []string{"t"},

						Usage: "which type of resource to describe, could be broker, topic, group",
					},
					&cli.StringFlag{
						Name:    "name",
						Aliases: []string{"n"},
						Value:   "",
						Usage:   "resource name, could be broker id, topic, consumer group ",
					},
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Value:   "",
						Usage:   "which topic to produce msg",
					},
					&cli.IntFlag{
						Name:    "partition",
						Aliases: []string{"p"},
						Value:   -1,
						Usage:   "which partition to produce msg, if not set, produced to a random partition",
					},
				},
				Action: func(c *cli.Context) error {
					rtype := c.String("type")
					rname := c.String("name")
					bootstrap := c.String("bootstrap-server")
					resourceType, err := kafka.ResourceTypeFromString(rtype)
					if err != nil {
						fmt.Printf("ERROR %s: Invalid resource type: %s\n", err, rtype)
						os.Exit(1)
					}
					ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
					if err != nil {
						panic(err)
					}

					wait, err := time.ParseDuration("10s")
					if err != nil {
						panic("ParseDuration(10s)")
					}
					results, err := ac.DescribeConfigs(ctx,
						[]kafka.ConfigResource{{Type: resourceType, Name: rname}},
						kafka.SetAdminRequestTimeout(wait))
					if err != nil {
						fmt.Printf("Failed to DescribeConfigs(%s, %s): %s\n",
							resourceType, rname, err)
						os.Exit(1)
					}

					for _, result := range results {
						fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
						for _, entry := range result.Config {
							fmt.Printf("%60s = %-60.60s   %-20s Read-only:%v Sensitive:%v\n",
								entry.Name, entry.Value, entry.Source,
								entry.IsReadOnly, entry.IsSensitive)
						}
					}

					topic := c.String("topic")
					partition := c.Int("partition")
					if topic != "" && partition != -1 {
						//showPartitionOffset()
					}
					ac.Close()
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func showPartitionOffset(c *kafka.Consumer, topic string, partition int) {
	committedOffsets, err := c.Committed([]kafka.TopicPartition{{
		Topic:     &topic,
		Partition: int32(partition),
	}}, 5000)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch offset: %s\n", err)
		os.Exit(1)
	}

	committedOffset := committedOffsets[0]

	fmt.Printf("Committed partition %d offset: %d\n", committedOffset.Partition, committedOffset.Offset)

	if committedOffset.Metadata != nil {
		fmt.Printf(" metadata: %s", *committedOffset.Metadata)
	} else {
		//fmt.Println("\n Looks like we fetch empty metadata. Ensure that librdkafka version > v1.1.0")
	}
}
