
resource "aws_cloudwatch_log_group" "logs" {
  name = "Totes_Bags"

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
  period              = 60
  threshold           = 1
  alarm_actions     = [aws_ses_event_destination.destination.arn]
}

resource "aws_ses_email_identity" "email" {
  email = "dataengineering@northcoders.com"
}

resource "aws_ses_configuration_set" "set" {
  name = "ConfigurationSet"
}

resource "aws_ses_event_destination" "destination" {
  name               = "Destination"
  configuration_set_name = aws_ses_configuration_set.set.name
  enabled            = true
  matching_types     = ["bounce", "send"]
}

