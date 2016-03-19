# Amazon kinesis producer 
> Batch producer for Kinesis built on top of the official Go AWS SDK  
and using the same aggregation format that [KPL][kpl-url] use.  
Note: this project start as a fork of [go-kinesis][fork-url]; if you are not intersting  
in the KPL aggregation logic, you probably want to check it out.

### Useful links
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]
- [Aggregation format](/aggregation-format.md])


### License
MIT


[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[go-kinesis]: https://github.com/tj/go-kinesis
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html

