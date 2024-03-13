import { Construct } from 'constructs';
import { aws_stepfunctions as sfn } from 'aws-cdk-lib';
import { aws_s3 as s3 } from 'aws-cdk-lib';
import * as cdk from "aws-cdk-lib"
import { aws_events as events } from 'aws-cdk-lib';
import { aws_events_targets as targets } from 'aws-cdk-lib';

export interface HelloMapProps {

}

export class HelloMap extends Construct {
  constructor(scope: Construct, id: string, props: HelloMapProps) {
    super(scope, id);

    // S3 Bucket
    const helloDMapDataBucket = new s3.Bucket(this, 'HelloDMapDataBucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      eventBridgeEnabled: true,
    })
    const helloDMapResultsBucket = new s3.Bucket(this, 'HelloDMapResultsBucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })


    // State Machine
    const distributedMap = new sfn.DistributedMap(this, 'DistributedMap', {

      maxConcurrency: 10000,
      itemReader: new sfn.S3CsvItemReader({
        bucket: helloDMapDataBucket,
        key: sfn.JsonPath.stringAt("$.detail.object.key"),
      }),
      itemBatcher: new sfn.ItemBatcher({
        maxItemsPerBatch: 1000
      }),
      resultWriter: new sfn.ResultWriter({
        bucket: helloDMapResultsBucket,
        prefix: "results"
      }),
      toleratedFailurePercentage: 1,
    })

    const highlyRated = new sfn.Pass(this, 'highly rated', {
      parameters: {
        "highrated.$": "$.Items[?(@.rating == '4.0' || @.rating == '5.0' )]"
      }
    })

    distributedMap.itemProcessor(highlyRated, {
      mode: sfn.ProcessorMode.DISTRIBUTED,
      executionType: sfn.ProcessorType.EXPRESS
    })

    const stateMachine = new sfn.StateMachine(scope, 'StateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(distributedMap),
      comment: 'Hello Distributed Map'
    })

    helloDMapDataBucket.grantRead(stateMachine)

    // https://github.com/aws/aws-cdk/issues/17472 これが対応していない。
    new events.Rule(scope, 'HelloDMapDataBucketEvent', {
      eventPattern: {
        source: ['aws.s3'],
        detailType: ["Object Created"],
        detail: {
          bucket: {
            name: [helloDMapDataBucket.bucketName],
          },
        },
      },
      targets: [new targets.SfnStateMachine(stateMachine)]
    })


  }
}