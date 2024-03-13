import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { HelloMap } from './constructs/hellomap';
import { ModuleProcessMultiFile } from './constructs/module-process-multi-file';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class WorkshopDistributedMapStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    new HelloMap(this, 'HelloMap', {})
    new ModuleProcessMultiFile(this, 'ModuleProcessMultiFile', {})
  }
}
