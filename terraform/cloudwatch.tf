
resource "aws_cloudwatch_log_group" "logs" {
  name = "Totes_Bags"
  retention_in_days = 60

  tags = {
    Environment = "production"
    Application = "serviceA"
  }
}


resource "aws_cloudwatch_log_metric_filter" "yada" {
  name           = "CatchError"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.logs.name

  metric_transformation {
    name      = "ErrorMetric"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alarm" {
  alarm_name          = "ErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorMetric"
  namespace           = "CustomLambdaMetrics"
  alarm_description   = "This metric notifies when an error occurs"
  statistic           = "Sum"
  period              = 30
  threshold           = 1
  alarm_actions     = [aws_sns_topic.monitoring.arn]
}

resource "aws_sns_topic" "monitoring" {
  name = "ErrorTopic"
}

resource "aws_sns_topic_subscription" "emailsubscription" {
  protocol = "email"
  endpoint = "sidleynorthcoders@gmail.com"
  topic_arn = aws_sns_topic.monitoring.arn
}
