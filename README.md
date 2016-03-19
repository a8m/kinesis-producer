# Amazon kinesis producer [![GoDoc][godoc-img]][godoc-url]
> A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK  
and using the same aggregation format that [KPL][kpl-url] use.  

<sub>__Note__: this project start as a fork of [go-kinesis][fork-url]; if you are not intersting 
in the KPL aggregation logic, you probably want to check it out.</sub>

### Useful links
- [Aggregation format][aggregation-format-url]
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]


### License
MIT

[godoc-url]: https://godoc.org/github.com/a8m/kinesis-producer
[godoc-img]: https://godoc.org/github.com/a8m/kinesis-producer?status.svg
[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[fork-url]: https://github.com/tj/go-kinesis
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html
[aggregation-format-url]: https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md
