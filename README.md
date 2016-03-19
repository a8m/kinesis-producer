# Amazon kinesis producer [![GoDoc][godoc-img]][godoc-url]
> A KPL-like Batch producer for Amazon Kinesis built on top of the official Go AWS SDK  
and using the same aggregation format that [KPL][kpl-url] use.  

>__Note__: this project start as a fork of [go-kinesis][fork-url]; if you are not intersting  
in the KPL aggregation logic, you probably want to check it out.

### Useful links
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]
- [Aggregation format](/aggregation-format.md])


### License
MIT
[godoc-url]: https://godoc.org/github.com/a8m/kinesis-producer
[godoc-img]: https://godoc.org/github.com/a8m/kinesis-producer?status.svg
[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[fork-url]: https://github.com/tj/go-kinesis
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html

