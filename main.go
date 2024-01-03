package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/athena"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func init() {
	log.SetFlags(0)
}

func runAthenaQuery(svc *athena.Athena, query string, outputLocation string) (*athena.StartQueryExecutionOutput, error) {
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("default"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(fmt.Sprintf("s3://sod-auctions/%s", outputLocation)),
		},
	}

	return svc.StartQueryExecution(input)
}

func waitForQueryToComplete(svc *athena.Athena, queryExecutionId string) error {
	for {
		input := &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryExecutionId),
		}
		output, err := svc.GetQueryExecution(input)
		if err != nil {
			return err
		}
		switch aws.StringValue(output.QueryExecution.Status.State) {
		case athena.QueryExecutionStateSucceeded:
			return nil
		case athena.QueryExecutionStateFailed:
			if strings.Contains(*output.QueryExecution.Status.StateChangeReason, "AlreadyExistsException") {
				log.Printf("query execution error: already exists")
				return nil
			}
			return errors.New("query execution failed")
		case athena.QueryExecutionStateCancelled:
			return errors.New("query execution cancelled")
		default:
			time.Sleep(time.Second)
		}
	}
}

func runPartitionQuery(svc *athena.Athena, dateInfo map[string]string) error {
	query := fmt.Sprintf("ALTER TABLE sod_auctions "+
		"ADD PARTITION (year='%[1]s', month='%[2]s', day='%[3]s', hour='%[4]s') "+
		"LOCATION 's3://sod-auctions/data/year=%[1]s/month=%[2]s/day=%[3]s/hour=%[4]s'",
		dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

	outputLocation := "results/partitioning"

	output, err := runAthenaQuery(svc, query, outputLocation)
	if err != nil {
		return err
	}

	return waitForQueryToComplete(svc, aws.StringValue(output.QueryExecutionId))
}

func runAggregationQuery(svc *athena.Athena, dateInfo map[string]string) (*athena.StartQueryExecutionOutput, error) {
	hour, _ := strconv.Atoi(dateInfo["hour"])

	layout := "2006-01-02 15Z"
	dateStr := fmt.Sprintf("%s-%s-%s %sZ", dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])
	date, err := time.Parse(layout, dateStr)
	if err != nil {
		return nil, err
	}

	timestamp := date.Format(time.RFC3339)

	query := fmt.Sprintf("SELECT '%s' AS timestamp, "+
		"realmId, auctionHouseId, "+
		"itemId, SUM(quantity) AS quantity, "+
		"MIN(buyoutEach) AS min, MAX(buyoutEach) AS max, APPROX_PERCENTILE(buyoutEach, 0.05) AS p05, "+
		"APPROX_PERCENTILE(buyoutEach, 0.1) AS p10, APPROX_PERCENTILE(buyoutEach, 0.25) AS p25, "+
		"APPROX_PERCENTILE(buyoutEach, 0.5) AS p50, APPROX_PERCENTILE(buyoutEach, 0.75) AS p75, "+
		"APPROX_PERCENTILE(buyoutEach, 0.9) AS p90 "+
		"FROM sod_auctions "+
		"WHERE year='%s' AND month='%s' AND day='%s' AND CAST(hour AS integer) = %d "+
		"GROUP BY realmId, auctionHouseId, itemId", timestamp, dateInfo["year"], dateInfo["month"], dateInfo["day"], hour)

	outputLocation := fmt.Sprintf("results/aggregates/interval=1/year=%s/month=%s/day=%s/hour=%s",
		dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

	return runAthenaQuery(svc, query, outputLocation)
}

func runDistributionQuery(svc *athena.Athena, dateInfo map[string]string) (*athena.StartQueryExecutionOutput, error) {
	query := fmt.Sprintf("SELECT realmId, auctionHouseId, itemId, buyoutEach, SUM(quantity) AS quantity "+
		"FROM sod_auctions WHERE year='%s' AND month='%s' AND day='%s' AND hour='%s' "+
		"GROUP BY realmId, auctionHouseId, itemId, buyoutEach",
		dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

	outputLocation := "results/price-distributions"

	return runAthenaQuery(svc, query, outputLocation)
}

func handler(ctx context.Context, event events.S3Event) error {
	for _, record := range event.Records {
		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			return fmt.Errorf("error decoding S3 object key: %v", err)
		}

		components := strings.Split(key, "/")
		dateInfo := make(map[string]string)
		for _, component := range components {
			parts := strings.Split(component, "=")
			if len(parts) == 2 {
				dateInfo[parts[0]] = parts[1]
			}
		}

		sess := session.Must(session.NewSession())
		svc := athena.New(sess)

		log.Printf("starting athena queries for file %s\n", key)

		log.Println("running partition query...")
		err = runPartitionQuery(svc, dateInfo)
		if err != nil {
			return fmt.Errorf("error occurred while running partioning query: %v", err)
		}

		log.Println("running aggregate query...")
		_, err = runAggregationQuery(svc, dateInfo)
		if err != nil {
			return fmt.Errorf("error occurred while running distribution query: %v", err)
		}

		log.Println("running distribution query..")
		_, err = runDistributionQuery(svc, dateInfo)
		if err != nil {
			return fmt.Errorf("error occurred while running distribution query: %v", err)
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
