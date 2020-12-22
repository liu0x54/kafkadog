package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	bootstrap  string
	topic      string
	numPart    int
	replicas   int
	createtpc  bool
	deletetpc  bool
	describers bool
	sendmsg    bool
	rsType     string
	rsName     string
)

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
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "bootstrap-server",
				Aliases: []string{"bs"},
				Value:   "localhost:9094",
				Usage:   "bootstrap server of kafka cluster",
			},
			&cli.StringFlag{
				Name:    "topic",
				Aliases: []string{"tp"},

				Usage: "topic to create",
			},
			&cli.IntFlag{
				Name:    "partitions",
				Aliases: []string{"pt"},
				Value:   10,
				Usage:   "the number of partitions",
			},
			&cli.IntFlag{
				Name:    "replicas",
				Aliases: []string{"rp"},
				Value:   3,
				Usage:   "the number of replicas",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "create",
				Aliases: []string{"cr"},
				Usage:   "create a topic",
				Action: func(c *cli.Context) error {
					fmt.Println("created topic: ", c.String("topic"))
					topic = c.String("topic")
					bootstrap = c.String("bootstrap-server")
					numPart = c.Int("partitions")
					replicas = c.Int("replicas")
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
				Name:    "produce",
				Aliases: []string{"p"},
				Usage:   "produce messages to the topic",
				Action: func(c *cli.Context) error {
					fmt.Println("produce message: ", c.Args().First())
					return nil
				},
			},
			{
				Name:    "template",
				Aliases: []string{"t"},
				Usage:   "options for task templates",
				Subcommands: []*cli.Command{
					{
						Name:  "add",
						Usage: "add a new template",
						Action: func(c *cli.Context) error {
							fmt.Println("new task template: ", c.Args().First())
							return nil
						},
					},
					{
						Name:  "remove",
						Usage: "remove an existing template",
						Action: func(c *cli.Context) error {
							fmt.Println("removed task template: ", c.Args().First())
							return nil
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println("run here!")
		log.Fatal(err)
	}

	/*
		results, err = ac.DescribeConfigs(ctx,
			[]kafka.ConfigResource{{Type: resourceType, Name: resourceName}},
			kafka.SetAdminRequestTimeout(dur))
		if err != nil {
			fmt.Printf("Failed to DescribeConfigs(%s, %s): %s\n",
				resourceType, resourceName, err)
			os.Exit(1)
		}

		// Print results
		for _, result := range results {
			fmt.Printf("%s %s: %s:\n", result.Type, result.Name, result.Error)
			for _, entry := range result.Config {
				// Truncate the value to 60 chars, if needed, for nicer formatting.
				fmt.Printf("%60s = %-60.60s   %-20s Read-only:%v Sensitive:%v\n",
					entry.Name, entry.Value, entry.Source,
					entry.IsReadOnly, entry.IsSensitive)
			}
		}*/

}
