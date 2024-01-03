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

func runPartitionQuery(svc *athena.Athena, tableName string, dateInfo map[string]string) error {
	query := fmt.Sprintf("ALTER TABLE %[1]s "+
		"ADD PARTITION (year='%[2]s', month='%[3]s', day='%[4]s', hour='%[5]s') "+
		"LOCATION 's3://sod-auctions/data/year=%[2]s/month=%[3]s/day=%[4]s/hour=%[5]s'",
		tableName, dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

	outputLocation := "results/partitioning"

	output, err := runAthenaQuery(svc, query, outputLocation)
	if err != nil {
		return err
	}

	return waitForQueryToComplete(svc, aws.StringValue(output.QueryExecutionId))
}

func runAggregationQuery(svc *athena.Athena, interval int, dateInfo map[string]string) (*athena.StartQueryExecutionOutput, error) {
	end, _ := strconv.Atoi(dateInfo["hour"])
	start := end - (interval - 1)

	layout := "2006-01-02 15Z"
	dateStr := fmt.Sprintf("%s-%s-%s %sZ", dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])
	date, err := time.Parse(layout, dateStr)
	if err != nil {
		return nil, err
	}

	timestamp := date.Format(time.RFC3339)
	year, _ := strconv.Atoi(date.Format("2006"))
	month, _ := strconv.Atoi(date.Format("01"))
	day, _ := strconv.Atoi(date.Format("02"))
	hour, _ := strconv.Atoi(date.Format("15"))
	dayOfWeek := int(date.Weekday())

	query := fmt.Sprintf("SELECT '%s' AS timestamp, "+
		"'%d' AS year, '%d' AS month, '%d' AS day, '%d' AS dayOfWeek, '%d' AS hour, "+
		"realmId, auctionHouseId, "+
		"itemId, SUM(quantity) AS quantity, "+
		"MIN(buyoutEach) AS min, MAX(buyoutEach) AS max, APPROX_PERCENTILE(buyoutEach, 0.05) AS p05, "+
		"APPROX_PERCENTILE(buyoutEach, 0.1) AS p10, APPROX_PERCENTILE(buyoutEach, 0.25) AS p25, "+
		"APPROX_PERCENTILE(buyoutEach, 0.5) AS p50, APPROX_PERCENTILE(buyoutEach, 0.75) AS p75, "+
		"APPROX_PERCENTILE(buyoutEach, 0.9) AS p90 "+
		"FROM sod_auctions "+
		"WHERE year='%s' AND month='%s' AND day='%s' AND CAST(hour AS integer) BETWEEN %d and %d "+
		"GROUP BY realmId, auctionHouseId, itemId", timestamp, year, month, day, dayOfWeek, hour,
		dateInfo["year"], dateInfo["month"], dateInfo["day"], start, end)

	outputLocation := fmt.Sprintf("results/aggregates/interval=%[1]d/year=%[2]s/month=%[3]s/day=%[4]s/hour=%[5]s",
		interval, dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

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

		log.Println("running partition query for table sod_auctions")
		err = runPartitionQuery(svc, "sod_auctions", dateInfo)
		if err != nil {
			return fmt.Errorf("error occurred while running partioning query: %v", err)
		}

		log.Println("running aggregate query for interval=1..")
		_, err = runAggregationQuery(svc, 1, dateInfo)
		if err != nil {
			return fmt.Errorf("error occurred while running distribution query: %v", err)
		}

		ahour, _ := strconv.Atoi(dateInfo["hour"])
		if ahour == 5 || ahour == 11 || ahour == 17 || ahour == 23 {
			log.Println("running aggregate query for interval=6..")
			_, err = runAggregationQuery(svc, 6, dateInfo)
			if err != nil {
				return fmt.Errorf("error occurred while running distribution query: %v", err)
			}
		}

		if ahour == 11 || ahour == 23 {
			log.Println("running aggregate query for interval=12..")
			_, err = runAggregationQuery(svc, 12, dateInfo)
			if err != nil {
				return fmt.Errorf("error occurred while running distribution query: %v", err)
			}
		}

		if ahour == 23 {
			log.Println("running aggregate query for interval=24..")
			_, err = runAggregationQuery(svc, 24, dateInfo)
			if err != nil {
				return fmt.Errorf("error occurred while running distribution query: %v", err)
			}
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
