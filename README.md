# Large-scale Data Processing with Step Functions

[Workshopのリンク](https://catalog.us-east-1.prod.workshops.aws/workshops/2a22e604-2f2e-4d7b-85a8-33b38c999234/en-US)


## Module1
以下のデータ（csv）をS3バケットに置いたら、それをトリガーにDistribute Mapが起動して処理。

[データのリンク](https://raw.githubusercontent.com/MengtingWan/marketBias/master/data/df_electronics.csv)

![](/docs/module1.drawio.svg)

## Module2
手動起動すると、S3に置いてあるcsvを処理。

インプットとしては以下を与える。

```JSON
{
  "maxConcurrency": 200,
  "maxItemsPerBatch": 200,
  "maxInputBytesPerBatch": 256,
}
```

![](/docs/module2.drawio.svg)

