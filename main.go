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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

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

func handler(ctx context.Context, event events.S3Event) error {
	for _, record := range event.Records {
		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			return fmt.Errorf("error decodeding S3 object key: %v", err)
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

		query := fmt.Sprintf(`
			ALTER TABLE sod_auctions ADD PARTITION (year='%[1]s', month='%[2]s', day='%[3]s', hour='%[4]s')
    		LOCATION 's3://sod-auctions/data/year=%[1]s/month=%[2]s/day=%[3]s/hour=%[4]s';`,
			dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

		log.Printf("partitioning directory for %s", key)
		output, err := runAthenaQuery(svc, query, "results/partitioning/")
		if err != nil {
			return fmt.Errorf("error occurred while running athena query: %v", err)
		}

		err = waitForQueryToComplete(svc, aws.StringValue(output.QueryExecutionId))
		if err != nil {
			return fmt.Errorf("error occurred during athena query execution: %s", err)
		}

		log.Println("starting aggregate query execution..")
		query = fmt.Sprintf(`
			SELECT year, month, day, hour, realmId, auctionHouseId, itemId, SUM(quantity) AS quantity, 
			MIN(buyout) AS min, MAX(buyout) AS max,
			APPROX_PERCENTILE(buyout, 0.05) AS p05, APPROX_PERCENTILE(buyout, 0.1) AS p10,
			APPROX_PERCENTILE(buyout, 0.25) AS p25, APPROX_PERCENTILE(buyout, 0.5) AS p50,
			APPROX_PERCENTILE(buyout, 0.75) AS p75, APPROX_PERCENTILE(buyout, 0.9) AS p90
			FROM sod_auctions
			WHERE year='%[1]s' AND month='%[2]s' AND day='%[3]s' AND hour='%[4]s'
			GROUP BY year, month, day, hour, realmId, auctionhouseId, itemId;
		`, dateInfo["year"], dateInfo["month"], dateInfo["day"], dateInfo["hour"])

		_, err = runAthenaQuery(svc, query, "results/aggregates/")
		if err != nil {
			return fmt.Errorf("error occurred while running athena query: %s", err)
		}
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
