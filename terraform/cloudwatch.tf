
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
  statistic           = "Sum"
  period              = 60
  threshold           = 1
  alarm_actions     = [aws_sns_topic.monitoring.arn]
}

resource "aws_sns_topic" "monitoring" {
  name = "ErrorTopic"
}

resource "aws_sns_topic_subscription" "sks"{
  protocol = "email"
  endpoint = "sidleynorthcoders@gmail.com"
  topic_arn = aws_sns_topic.monitoring.arn
}

# resource "aws_ses_configuration_set" "set" {
#   name = "ConfigurationSet"
# }

# resource "aws_ses_event_destination" "destination" {
#   name               = "Destination"
#   configuration_set_name = aws_ses_configuration_set.set.name
#   enabled            = true
#   matching_types     = ["bounce", "send"]
#   cloudwatch_destination {
#       dimension_name = "ErrorAlarm"
#       default_value  = "Error"
#       value_source = "emailHeader"
#   }
# }
