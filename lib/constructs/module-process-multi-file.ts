import { Construct } from 'constructs';
import { aws_stepfunctions as sfn } from 'aws-cdk-lib';
import { aws_stepfunctions_tasks as tasks } from 'aws-cdk-lib';
import { aws_s3 as s3 } from 'aws-cdk-lib';
import { aws_lambda as lambda } from 'aws-cdk-lib';
import * as cdk from "aws-cdk-lib"
import { aws_dynamodb as dynamodb } from 'aws-cdk-lib';
import { aws_s3_deployment as s3deploy } from 'aws-cdk-lib';

export interface ModuleProcessMultiFileProps {

}

export class ModuleProcessMultiFile extends Construct {
  constructor(scope: Construct, id: string, props: ModuleProcessMultiFileProps) {
    super(scope, id);

    // S3 Bucket (input)
    const multiFileDataBucket = new s3.Bucket(this, 'MultiFileDataBucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      eventBridgeEnabled: true,
    })


    new s3deploy.BucketDeployment(this, 'ImageDeploy', {
      sources: [s3deploy.Source.asset("data/noah.zip")],
      destinationBucket: multiFileDataBucket,
      destinationKeyPrefix: "csv/by_station/"
    })


    // S3 Bucket (output)
    const multiFileResultsBucket = new s3.Bucket(this, 'MultiFileResultsBucket', {
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })


    // Mapのリソース
    const resultsDynamoDBTable = new dynamodb.Table(this, 'ResultsDynamoDBTable', {
      partitionKey: {
        name: 'pk',
        type: dynamodb.AttributeType.STRING
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })

    const highPrecipitationFunction = new lambda.Function(this, 'HighPrecipitationFunction', {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'index.lambda_handler',
      code: lambda.Code.fromAsset("lambda/highPrecipitation"),
      environment: {
        RESULTS_DYNAMODB_TABLE_NAME: resultsDynamoDBTable.tableName,
        INPUT_BUCKET_NAME: multiFileDataBucket.bucketName
      },
      memorySize: 2048,
      timeout: cdk.Duration.seconds(600)
    })

    multiFileDataBucket.grantRead(highPrecipitationFunction)
    resultsDynamoDBTable.grantWriteData(highPrecipitationFunction)



    // Distributed Map を定義
    const distributedMap = new sfn.DistributedMap(this, 'MultiFileDistributedMap', {
      label: 'Ditributedmaphighprecipitation',
      itemReader: new sfn.S3ObjectsItemReader({
        bucket: multiFileDataBucket,
        prefix: "csv/by_station/noah"
      }),
      itemSelector: {
        "Key": sfn.JsonPath.stringAt('$$.Map.Item.Value.Key')
      },
      itemBatcher: new sfn.ItemBatcher({
        // 固定値の場合
        // maxItemsPerBatch: 100,
        // maxInputBytesPerBatch: 262144,

        // 動的に変更したい場合
        maxItemsPerBatchPath: sfn.JsonPath.stringAt('$.maxItemsPerBatch'),
        maxInputBytesPerBatchPath: sfn.JsonPath.stringAt('$.maxInputBytesPerBatch')
      }),

      // 固定値の場合
      // maxConcurrency: 1000,
      // 動的に変更したい場合
      maxConcurrencyPath: sfn.JsonPath.stringAt('$.maxConcurrency'),

      mapExecutionType: sfn.StateMachineType.EXPRESS, // STANDARD or EXPRESS を選択。デフォルトは STANDARD
      resultSelector: {
        'result_bucket': sfn.JsonPath.stringAt('$.ResultWriterDetails.Bucket')
      },
      resultWriter: new sfn.ResultWriter({
        bucket: multiFileResultsBucket,
        prefix: "results"
      }),
      // 固定値の場合
      toleratedFailureCount: 5,
      toleratedFailurePercentage: 5,
      // 動的に変更したい場合
      // toleratedFailureCountPath: sfn.JsonPath.stringAt('$.toleratedFailureCount'),
      // toleratedFailurePercentagePath: sfn.JsonPath.stringAt('$.toleratedFailurePercentage'),
    })


    // Lambda関数を呼び出すステートを定義
    const highPrecipitationFunctionTask = new tasks.LambdaInvoke(this, 'HighPrecipitationFunctionTask', {
      lambdaFunction: highPrecipitationFunction,
      payload: sfn.TaskInput.fromJsonPathAt('$'),
      outputPath: sfn.JsonPath.stringAt('$.Payload'),
      retryOnServiceExceptions: false
    })

    highPrecipitationFunctionTask.addRetry({
      maxAttempts: 6,
      backoffRate: 2,
      interval: cdk.Duration.seconds(2),
      errors: [
        "Lambda.ServiceException",
        "Lambda.AWSLambdaException",
        "Lambda.SdkClientException",
        "Lambda.TooManyRequestsException"
      ]
    })

    // DistributedMapにItemProcessorを設定
    distributedMap.itemProcessor(highPrecipitationFunctionTask, {
    })

    // ステートマシンを定義
    new sfn.StateMachine(scope, 'MultiFileStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(distributedMap),
    })


  }
}